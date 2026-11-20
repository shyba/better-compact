import { expect, test } from "bun:test"
import { createHash } from "node:crypto"
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { drainGenerations, generationCheckpointed, reconcileAppliedGenerations, stageGeneration, type GenerationIntent } from "../src/clickhouse-generation-outbox.js"
import { outboxDirectory, uploadClickHouseFile } from "../src/clickhouse-sync.js"

const authority = { store: "clickhouse" as const, protocol: 1 as const, authority_id: "test-authority-000001", max_segment_bytes: 4 }
const hash = (value: string) => createHash("sha256").update(value).digest("hex")

test("concurrent conflicting first intents cannot overwrite each other", async () => {
  const root=await mkdtemp(path.join(os.tmpdir(),"ch-gen-race-")), previous=process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX=root
  try {
    const intent:GenerationIntent={authority:authority.authority_id,source_id:"e".repeat(32),file_id:"same",path:"file.jsonl",
      start:0,size:4,sha256:hash("aaaa"),parent:null,predecessor:null,mtime_ms:1,segment_bytes:4,created_at:1}
    const other={...intent,sha256:hash("bbbb")}
    const results=await Promise.allSettled([stageGeneration(intent,authority,4096),stageGeneration(other,authority,4096)])
    expect(results.filter(r=>r.status==="fulfilled")).toHaveLength(1)
    expect(results.filter(r=>r.status==="rejected")).toHaveLength(1)
    const names=await readdir(outboxDirectory(authority))
    const file=names.find(name=>name.endsWith(".generation.json"))!
    const saved=JSON.parse(await readFile(path.join(outboxDirectory(authority),file),"utf8")) as GenerationIntent
    expect([intent.sha256,other.sha256]).toContain(saved.sha256)
    const lost=saved.sha256===intent.sha256?other:intent
    await expect(stageGeneration(lost,authority,4096)).rejects.toThrow("immutable generation intent conflict")
    expect(JSON.parse(await readFile(path.join(outboxDirectory(authority),file),"utf8")).sha256).toBe(saved.sha256)
  } finally {
    if (previous===undefined) delete process.env.BETTER_SYNC_OUTBOX; else process.env.BETTER_SYNC_OUTBOX=previous
    await rm(root,{recursive:true,force:true})
  }
})

test("generation intent waits for durable segments and survives lost receipt and checkpoint crash", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "ch-gen-")), previous = process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX = root
  let generationCalls=0, applied=0
  const server=Bun.serve({ port:0, async fetch(request) {
    if (new URL(request.url).pathname === "/generation") {
      generationCalls++
      const body=await request.json() as Record<string,unknown>
      if (generationCalls===1) return new Response("lost acknowledgement", { status:503 })
      return Response.json({ ...body, authority_id:authority.authority_id, store:"clickhouse", committed:true })
    }
    const body=await request.arrayBuffer()
    return Response.json({ authority_id:authority.authority_id, store:"clickhouse", committed:true,
      sha256:request.headers.get("x-vcc-sha256"),source_id:request.headers.get("x-vcc-source-id"),
      source_kind:request.headers.get("x-vcc-source-kind"),locator:request.headers.get("x-vcc-source-locator"),
      file_id:decodeURIComponent(new URL(request.url).pathname.slice("/file/".length)),path:request.headers.get("x-vcc-path"),
      start:Number(request.headers.get("x-vcc-start")),size:body.byteLength,full:request.headers.get("x-vcc-full")==="true" })
  } })
  try {
    const source="a".repeat(32), file=path.join(root,"source.jsonl"), endpoint=server.url.toString()
    const intent:GenerationIntent={ authority:authority.authority_id,source_id:source,file_id:"version",path:"source.jsonl",
      start:0,size:7,sha256:hash("payload"),parent:null,predecessor:null,mtime_ms:100,segment_bytes:4,created_at:1 }
    expect(await generationCheckpointed(authority,source,"version")).toBe(false)
    await stageGeneration(intent,authority,4096)
    expect(await generationCheckpointed(authority,source,"version")).toBe(false)
    await expect(drainGenerations({endpoint,token:"token"},authority,()=>{throw new Error("not ready")})).resolves.toMatchObject({pending:1,applied:0})
    expect(generationCalls).toBe(0)
    await writeFile(file,"payload")
    await uploadClickHouseFile({endpoint,token:"token",fileID:"version",sourcePath:"source.jsonl",filename:file,
      size:7,start:0,full:true,source:{id:source,kind:"pi-jsonl",locator:root}},authority,4096)
    await rm(file)
    await expect(drainGenerations({endpoint,token:"token"},authority,()=>{})).rejects.toThrow("503")
    await expect(drainGenerations({endpoint,token:"token"},authority,()=>{applied++;throw new Error("checkpoint crash")})).rejects.toThrow("checkpoint crash")
    expect(generationCalls).toBe(2)
    await expect(drainGenerations({endpoint,token:"token"},authority,()=>{applied++})).resolves.toMatchObject({pending:0,applied:1})
    expect(applied).toBe(2);expect(generationCalls).toBe(2)
    expect(await generationCheckpointed(authority,source,"version")).toBe(true)
    await expect(generationCheckpointed(authority,source,"version",{path:"source.jsonl",size:7,sha256:"0".repeat(64)})).rejects.toThrow("checkpoint identity mismatch")
    await drainGenerations({endpoint,token:"token"},authority,()=>{throw new Error("already applied")})
    const names=await readdir(outboxDirectory(authority))
    expect(names.filter(name=>name.endsWith(".body"))).toHaveLength(2)
    const entry=JSON.parse(await readFile(path.join(outboxDirectory(authority),names.find(name=>name.endsWith(".generation.json"))!),"utf8"))
    expect(entry.committed_at).toBeGreaterThan(0);expect(entry.applied_at).toBeGreaterThan(0)
    await expect(stageGeneration({...intent,sha256:"0".repeat(64)},authority,4096)).rejects.toThrow("immutable generation intent conflict")
  } finally {
    server.stop(true)
    if (previous===undefined) delete process.env.BETTER_SYNC_OUTBOX; else process.env.BETTER_SYNC_OUTBOX=previous
    await rm(root,{recursive:true,force:true})
  }
})

test("older restored receiver replays retained segments and re-verifies without releasing bytes", async () => {
  const root=await mkdtemp(path.join(os.tmpdir(),"ch-gen-restore-")), previous=process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX=root
  const capable={...authority,generation_status_protocol:1}
  let generation:Record<string,unknown>|undefined, received=0
  const server=Bun.serve({port:0,async fetch(request) {
    const url=new URL(request.url)
    if (url.pathname==="/generation/status") return generation ? Response.json({...generation,authority_id:authority.authority_id,store:"clickhouse",committed:true}) : new Response("",{status:404})
    if (url.pathname==="/verify") {
      const body=await request.json() as {files:Array<{file_id:string}>}
      return Response.json({store:"clickhouse",authority_id:authority.authority_id,files:[{file_id:body.files[0]!.file_id,archive:{status:generation && received>=2 ? "verified":"mismatch"}}]})
    }
    if (url.pathname==="/generation") {
      generation=await request.json() as Record<string,unknown>
      return Response.json({...generation,authority_id:authority.authority_id,store:"clickhouse",committed:true})
    }
    const body=await request.arrayBuffer();received++
    return Response.json({authority_id:authority.authority_id,store:"clickhouse",committed:true,
      sha256:request.headers.get("x-vcc-sha256"),source_id:request.headers.get("x-vcc-source-id"),
      source_kind:request.headers.get("x-vcc-source-kind"),locator:request.headers.get("x-vcc-source-locator"),
      file_id:decodeURIComponent(url.pathname.slice("/file/".length)),path:request.headers.get("x-vcc-path"),
      start:Number(request.headers.get("x-vcc-start")),size:body.byteLength,full:request.headers.get("x-vcc-full")==="true"})
  }})
  try {
    const source="c".repeat(32),file=path.join(root,"source.jsonl"),endpoint=server.url.toString()
    await writeFile(file,"payload")
    const intent:GenerationIntent={authority:authority.authority_id,source_id:source,file_id:"version",path:"source.jsonl",
      start:0,size:7,sha256:hash("payload"),parent:null,predecessor:null,mtime_ms:1,segment_bytes:4,created_at:1}
    await stageGeneration(intent,authority,4096)
    await uploadClickHouseFile({endpoint,token:"token",fileID:intent.file_id,sourcePath:intent.path,filename:file,
      size:7,start:0,full:true,source:{id:source,kind:"pi-jsonl",locator:root}},authority,4096)
    await drainGenerations({endpoint,token:"token"},authority,()=>{})
    generation=undefined;received=0 // restored before this generation was inserted
    expect(await reconcileAppliedGenerations({endpoint,token:"token"},capable)).toEqual({checked:1,repaired:1})
    expect(received).toBe(2)
    expect((await readdir(outboxDirectory(authority))).filter(x=>x.endsWith(".body"))).toHaveLength(2)
    received=0 // receipt exists but raw references/chunks were lost
    expect(await reconcileAppliedGenerations({endpoint,token:"token"},capable)).toEqual({checked:1,repaired:1})
    expect(received).toBe(2)
  } finally {
    server.stop(true)
    if (previous===undefined) delete process.env.BETTER_SYNC_OUTBOX;else process.env.BETTER_SYNC_OUTBOX=previous
    await rm(root,{recursive:true,force:true})
  }
})

test("recovery follows predecessor links even if timestamps are reversed", async () => {
  const root=await mkdtemp(path.join(os.tmpdir(),"ch-gen-order-")), previous=process.env.BETTER_SYNC_OUTBOX
  process.env.BETTER_SYNC_OUTBOX=root
  const order:string[]=[], committed=new Map<string,Record<string,unknown>>()
  let verifications=0
  const server=Bun.serve({port:0,async fetch(request) {
    if (new URL(request.url).pathname==="/generation/status") {
      const body=await request.json() as Record<string,unknown>, item=committed.get(String(body.file_id))
      return item ? Response.json({...item,authority_id:authority.authority_id,store:"clickhouse",committed:true}) : new Response("",{status:404})
    }
    if (new URL(request.url).pathname==="/verify") {
      const body=await request.json() as {files:Array<{file_id:string}>};verifications++
      return Response.json({store:"clickhouse",authority_id:authority.authority_id,files:[{file_id:body.files[0]!.file_id,archive:{status:"verified"}}]})
    }
    if (new URL(request.url).pathname==="/generation") {
      const body=await request.json() as Record<string,unknown>;order.push(String(body.file_id));committed.set(String(body.file_id),body)
      return Response.json({...body,authority_id:authority.authority_id,store:"clickhouse",committed:true})
    }
    const body=await request.arrayBuffer()
    return Response.json({authority_id:authority.authority_id,store:"clickhouse",committed:true,
      sha256:request.headers.get("x-vcc-sha256"),source_id:request.headers.get("x-vcc-source-id"),
      source_kind:request.headers.get("x-vcc-source-kind"),locator:request.headers.get("x-vcc-source-locator"),
      file_id:decodeURIComponent(new URL(request.url).pathname.slice("/file/".length)),path:request.headers.get("x-vcc-path"),
      start:Number(request.headers.get("x-vcc-start")),size:body.byteLength,full:request.headers.get("x-vcc-full")==="true"})
  }})
  try {
    const source="b".repeat(32),file=path.join(root,"source.jsonl"),endpoint=server.url.toString()
    await writeFile(file,"abcdef")
    const base={authority:authority.authority_id,source_id:source,path:"source.jsonl",mtime_ms:100,segment_bytes:4}
    const first:GenerationIntent={...base,file_id:"first",start:0,size:3,sha256:hash("abc"),parent:null,predecessor:null,created_at:2}
    const second:GenerationIntent={...base,file_id:"second",start:3,size:6,sha256:hash("abcdef"),parent:"first",predecessor:"first",created_at:1}
    await stageGeneration(second,authority,8192);await stageGeneration(first,authority,8192)
    for (const intent of [first,second]) await uploadClickHouseFile({endpoint,token:"token",fileID:intent.file_id,sourcePath:intent.path,filename:file,
      size:intent.size,start:intent.start,full:intent.parent===null,source:{id:source,kind:"pi-jsonl",locator:root}},authority,8192)
    await drainGenerations({endpoint,token:"token"},authority,()=>{})
    expect(order).toEqual(["first","second"])
    expect(await reconcileAppliedGenerations({endpoint,token:"token"},{...authority,generation_status_protocol:1})).toEqual({checked:2,repaired:0})
    expect(verifications).toBe(1) // final leaf checks the complete prefix chain
  } finally {
    server.stop(true)
    if (previous===undefined) delete process.env.BETTER_SYNC_OUTBOX;else process.env.BETTER_SYNC_OUTBOX=previous
    await rm(root,{recursive:true,force:true})
  }
})
