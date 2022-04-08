import db_sqlite
import db_mysql
import tables
import os
import strutils
import sequtils
import strformat
import sugar
import logging
var logger = newConsoleLogger(fmtStr="$datetime | $levelname | ")


let sample_type = {
  "C": "CF",
  "T": "FT",
  "F": "FP",
  "B": "BL",
  "M": "MY",
  "X": "XS",
  "N": "NJ",
  "K": "KL",
  "Y": "YS",
  "W": "FT",
  "O": "OP",
  "I": "OP",
  "D": "FP",
  "S": "GS"
}.toTable()

# let pair_panel = "RD30|RD23|RD16|RD43|RD60".split("|")

type
  Units* = object
    unitCol: seq[string]
    runID*: string
    fqDir*: string
    mergeDir*: string
    anaDir*: string

proc `runid=`*(this: var Units, val: string) = 
  this.runID = val

proc `fqdir=`*(this: var Units, val: string) =
  this.fqDir = val

proc `mergedir=`*(this: var Units, val: string) = 
  this.mergeDir = val

proc `anadir=`*(this: var Units, val: string) = 
  this.anaDir = val

proc search_fq(this: var Units, project, sample: string): seq[string] {.discardable.} = 
  # 检索单个样本fastq文件路径
  # let run_fq_dir = joinPath(this.fqDir, "20" & this.runID[0..<4], this.runID)
  let paths = toSeq(walkFiles(&"{this.fqDir}/{project}/{sample}*gz"))
  return paths

proc get_pair(this: var Units, ss: OrderedTable): CountTable[string] =
  var paired = initCountTable[string]()
  for record in ss.values:
    paired.inc( record["Order_ID"] )
  paired.del("-")  # 湘雅NCPC
  return paired

proc to_units*(this: var Units, ss: OrderedTable, db: db_sqlite.DbConn) {.discardable.} =
  let units = open( joinPath(this.anaDir, "units.tsv") , fmWrite)
  defer: units.close()

  this.unitCol = @["PatientID",
    "SampleID",
    "PairType",
    "RunID",
    "Panel",
    "Product_Code",
    "SampleType",
    "Project",
    "R1path",
    "R2path",
    "IFpair",
    "Pool",
  ]
  units.writeLine(this.unitCol.join("\t"))

  # let lims = db_mysql.open("172.16.157.158", "xiaoyongliang", "xiaoyongliang", "result_database")
  # defer: lims.close()

  let paired = this.get_pair(ss)

  for record in ss.values:
    let
      SampleID = record["Sample_ID"]
      OrderID = if record["Order_ID"] != "-": record["Order_ID"] else: SampleID
      Product_Code = record["Product_Code"]
      Project = record["Sample_Project"]
      Panel = Project.split("_")[0]
      SampleType = sample_type[record["Description"]]
      ID = ""
    # var ID = lims.getValue(sql"SELECT o_patientIdentity FROM lims3_orders_data where orderCode=?", OrderID)

    var 
      paths = this.search_fq(Project, SampleID)
      R1path = paths[0]
      R2path = paths[1]

    # 先插入数据
    db.exec(sql"INSERT INTO sample_status (OrderID, SampleID, RunID, ID, Product_Code, R1path, R2path, Panel, SampleType) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      OrderID, SampleID, this.runID, ID, Product_Code, R1path, R2path, Panel, SampleType)

    let db_fq = db.getAllRows(sql"SELECT SampleID, R1path, R2path from sample_status where OrderID = ?", SampleID)
    if db_fq.len > 1:
      R1path = joinPath(this.mergeDir, this.runID, splitPath(R1path).tail)
      R2path = joinPath(this.mergeDir, this.runID, splitPath(R2path).tail)
      var
        r1paths = db_fq.map(x => x[2]).join(" ")
        r2paths = db_fq.map(x => x[3]).join(" ")
      logger.log(lvlInfo, &"cat {r1paths} > {R1path}")
      discard execShellCmd( &"cat {r1paths} > {R1path}" )
      logger.log(lvlInfo, &"cat {r2paths} > {R2path}")
      discard execShellCmd( &"cat {r2paths} > {R2path}" )

    var
      PairType = "Case"
      IFpair = "Single"
      Pool = "."
    if paired[OrderID] > 1:
      if SampleType in ["BL", "KL", "OP"]:
        PairType = "Control"
        IFpair = "Pair"
      else:
        IFpair = "Pair"

    units.writeLine(@[OrderID, 
      SampleID, 
      PairType, 
      this.runID, 
      Panel,
      Product_Code,
      SampleType,
      Project,
      R1path,
      R2path,
      IFpair,
      Pool].join("\t"))


when isMainModule:
  import samplesheet

  var s: SampleSheet
  s.parse_samplesheet(paramStr(1))

  # initDB(db)

  var u: Units
  u.runid = "220324_NDX550519_RUO_0300_AHGNJKBGXL"
  u.fqdir = "/data/Rawdata/Simceredx/2022"
  u.mergedir = "/data/Analysis/Production/Work/xuhao/nim_job/monitor_ngs"
  u.anadir = "/data/Analysis/Production/Work/xuhao/nim_job/monitor_ngs"
  u.to_units(s.sheetTab, db)

  echo u.get_pair(s.sheetTab)

  echo $u.unitCol
