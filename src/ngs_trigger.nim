import os
import db_sqlite
import strformat
import parsecfg

import docopt
import unpack
import logging
var logger = newConsoleLogger(fmtStr="$datetime | $levelname | ")
# 导入自定义模块
import samplesheet  # samplesheet只与拆分相关，并不需要本地库的信息
import units  # 用于生成units文件

const doc = """
ngs_trigger - a ngs pipeline trigger

Usage:
  ngs_trigger init
  ngs_trigger -r RunID -s SampleSheet [-b BclDirectory]
  ngs_trigger --runid RunID --samplesheet SampleSheet [--bcldir BclDirectory]

Options:
  init                              初始化本地sqlite库,创建run_status和sample_status两张表
  -b, --bcldir BclDirectory         bcl下机路径 [default: bcldir]
  -s, --samplesheet SampleSheet     SampleSheet文件,CSV格式
  -r, --runid RunID                 批次RunID号,如220324_NDX550519_RUO_0300_AHGNJKBGXL
"""

type
  Inputs = object
    anadir, mergedir, fqdir, pipedir: string
    snakemake, bcl2fastq: string
    bcldir, runid, samplesheet: string
    db: DbConn


proc check_bcl2fastq_status(runid: string, db: DbConn): string =
  # 不要检查路径了。。只检查表
  return db.getValue(sql"SELECT Fastq FROM run_status WHERE RunID=?", runid)


proc check_analysis_status(runid: string, db: DbConn): string =
  return db.getValue(sql"SELECT Analysis FROM run_status WHERE RunID=?", runid)


proc run_bcl2fastq(inputs: Inputs) =
  # unpack这个inputs对象
  inputs.unpackObject(fqdir, anadir, db, runid, bcl2fastq, bcldir, samplesheet)

  if not dirExists(fqdir): createDir(fqdir)
  if not dirExists(anadir): createDir(anadir)

  db.exec(sql"INSERT INTO run_status (RunID, Fastq) VALUES (?, ?)", runid, "wait")

  # samplesheet是绝对路径
  # bcldir包含runid
  let b2f = &"""{bcl2fastq} \
    -R {bcldir} \
    -o {fqdir} \
    --sample-sheet {samplesheet} \
    --barcode-mismatches 0 \
    --no-lane-splitting \
    > { joinPath(anadir, runid & "_bcl.log") } 2>&1
  """

  #测试调用shell命令
  let ls = "sleep 2"

  echo b2f
  if execShellCmd(ls) == 0:
    logger.log(lvlInfo, &"runid: {runid}, bcl2fastq completed")
    db.exec(sql"UPDATE run_status SET Fastq = ? where RunID = ?", "ok", runid)
  else:
    logger.log(lvlInfo, &"runid: {runid}, bcl2fastq failed, check {anadir}/{runid}_bcl.log")
    db.exec(sql"UPDATE run_status SET Fastq = ? where RunID = ?", "err", runid)
    quit()


proc run_snakemake(inputs: Inputs) =
  # create(tmp/key)
  # defer: remove(tmp/key)

  # 用来生成用于投递或者local run的snakemake脚本
  inputs.unpackObject(anadir, db, runid, snakemake, pipedir)

  # db.exec(sql"INSERT INTO run_status (RunID, Analysis) VALUES (?, ?)", runid, "wait")
  db.exec(sql"UPDATE run_status SET Analysis = ? where RunID = ?", "wait", runid)

  let smk = &"""{snakemake} \
    -s { joinPath(pipedir, "NGS.pipeline.snakefile.smk.py") } \
    --profile {pipedir} \
    --config info={anadir}/units.tsv \
    outdir={anadir} \
    msg=AIDA \
    user=AIDA
  """
  echo smk

    #测试调用shell命令
  let ls = "sleep 2"

  if execShellCmd(ls) == 0:
    logger.log(lvlInfo, &"runid: {runid}, analysis completed")
    db.exec(sql"UPDATE run_status SET Analysis = ? where RunID = ?", "ok", runid)
  else:
    logger.log(lvlInfo, &"runid: {runid}, analysis failed, check {anadir}/{runid}_bcl.log")
    db.exec(sql"UPDATE run_status SET Analysis = ? where RunID = ?", "err", runid)
    quit()


proc initDB(db: db_sqlite.DbConn) = 
  # defer: db.close()  # 这个连接不能关，主流程结束时再关吧，只需要打开一次

  db.exec(sql"DROP TABLE IF EXISTS sample_status") #逗号连接两张表报错
  db.exec(sql"DROP TABLE IF EXISTS run_status")

  db.exec(sql"""CREATE TABLE sample_status(
    OrderID char(20),
    SampleID char(30),
    RunID char(40),
    ID char(25),
    Product_Code char(20),
    R1path Char(200),
    R2path char(200),
    Panel char(20),
    SampleType char(10),
    primary key(SampleID,RunID)
  )""")

  # 没有测序的状态 RTAComplete char(4)
  db.exec(sql"""CREATE TABLE run_status(
    RunID char(40),
    Fastq char(4),
    Analysis char(4),
    primary key(RunID)
  )""")

  # 插入数据测试
  db.exec(sql"INSERT INTO run_status (RunID, Fastq) VALUES (?, ?)", 
  "190529_A00812_0017_BHKHNLDMXX", "ok")

  db.exec(sql"INSERT INTO sample_status (OrderID, SampleID, RunID, ID, Product_Code, R1path, R2path, Panel, SampleType) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    "B10000704", 
    "B10000704T1D1L1", 
    "190529_A00812_0017_BHKHNLDMXX", 
    "13063519800516154X", 
    "CPX05094", 
    "/data/Rawdata/Simceredx/2019/201905/190529_A00812_0017_BHKHNLDMXX/RD03_LCP69/B10000704T1D1L1_S136_R1_001.fastq.gz", 
    "/data/Rawdata/Simceredx/2019/201905/190529_A00812_0017_BHKHNLDMXX/RD03_LCP69/B10000704T1D1L1_S136_R2_001.fastq.gz", 
    "RD03", 
    "FT")

  logger.log(lvlInfo, "Init ngs_monitor.db completed")



proc main() =
  if not fileExists("ngs_config.ini"):
    logger.log(lvlError, "no ngs_config.ini in the same path")
    quit()
  # 配置文件
  var
    cfg = loadConfig("ngs_config.ini")
    anadir = cfg.getSectionValue("Directory", "anadir")
    mergedir = cfg.getSectionValue("Directory", "mergedir")
    fqdir = cfg.getSectionValue("Directory", "fqdir")
    pipedir = cfg.getSectionValue("Directory", "pipedir")
    snakemake = cfg.getSectionValue("Software", "snakemake")
    bcl2fastq = cfg.getSectionValue("Software", "bcl2fastq")

  # 本地库
  let db = db_sqlite.open("ngs_monitor.db", "", "", "")
  defer: db.close()

  # 命令行参数
  let args = docopt(doc, version = "0.1.0")
  if args["init"]:
    initDB(db)
    quit()
  let
    bcldir = $args["--bcldir"]
    runid = $args["--runid"]
    samplesheet = $args["--samplesheet"]

  fqdir = joinPath(fqdir, "20" & runid[0..<4], runid)  # 具体的fastq路径
  anadir = joinPath(anadir, "20" & runid[0..<4], runid)

  # 第一步先拆分
  case check_bcl2fastq_status(runid, db)
  of "ok":
    logger.log(lvlInfo, &"runid: {runid}, bcl2fastq already done")
  of "wait":
    logger.log(lvlInfo, &"runid: {runid}, bcl2fastq is running")
  else:
    logger.log(lvlInfo, &"runid: {runid}, bcl2fastq start")
    var inputs = Inputs(fqdir:fqdir, anadir:anadir, db:db, runid:runid, 
      bcl2fastq:bcl2fastq, bcldir:bcldir, samplesheet:samplesheet)
    run_bcl2fastq(inputs)


  # 拆分完成，第二步生成units
  var s: SampleSheet
  s.parse_samplesheet(samplesheet)
  var u = Units(runid:runid, fqdir:fqdir, mergedir:mergedir, anadir:anadir)
  try:
    u.to_units(s.sheetTab, db)
    logger.log(lvlInfo, &"runid: {runid}, units generated, check {anadir}/units.tsv")
  except:
    logger.log(lvlError, &"runid: {runid}, units generate failed, check {anadir}/units.tsv")
    quit()


  # 开始分析，生成snakemake的运行脚本
  case check_analysis_status(runid, db)
  of "ok":
    logger.log(lvlInfo, &"runid: {runid}, analysis already done")
  of "wait":
    logger.log(lvlInfo, &"runid: {runid}, analysis is running")
  else:
    logger.log(lvlInfo, &"runid: {runid}, analysis start")
    var inputs = Inputs(anadir:anadir, db:db, runid:runid, 
      snakemake:snakemake, pipedir:pipedir)
    run_snakemake(inputs)
    # 这个地方有一些日志传输，以及钉钉输出


  echo "Good Job!!!!"


when isMainModule:
  main()
