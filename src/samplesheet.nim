import parsecsv
import streams
import strformat
import strutils
import tables
import logging
import sets

var logger = newConsoleLogger(fmtStr="$datetime | $levelname | ")

type
  SampleSheet* = object
    sheetTab*: OrderedTable[string, OrderedTable[string, string]]
    sheetHead*: string

proc parse_samplesheet*(this: var SampleSheet, samplesheet: string) = 
  # OrderedTable[string, OrderedTable[string, string]] {.discardable.} =
  # 解析samplesheet文件，返回units需要的结果
  var s = newFileStream(samplesheet, fmRead)
  if s == nil:
    logger.log(lvlError, "duplicate sample name: " & samplesheet)
    quit()

  # this.sheetHead = ""

  # 把这个文件迭代器消耗到[Data]
  var line = ""
  while s.readLine(line):
    addf(this.sheetHead, line)
    if line.startsWith("[Data]"):
      break
    addf(this.sheetHead, "\n")

  var
    sampleRecord: OrderedTable[string, string]
    sampleSheetTab: OrderedTable[string, OrderedTable[string, string]]

  var x: CsvParser
  open(x, s, samplesheet)
  x.readHeaderRow()
  while x.readRow():
    for col in items(x.headers):
      sampleRecord[col] = x.rowEntry(col)
    if sampleSheetTab.hasKey( sampleRecord["Sample_ID"] ):
      logger.log(lvlError, "duplicate sample name: " & sampleRecord["Sample_ID"])
      quit()  # system是默认导入的？
    sampleSheetTab[ sampleRecord["Sample_ID"] ] = sampleRecord
  x.close()

  this.sheetTab = sampleSheetTab
  # return sampleSheetTab


proc dup_index*(this: SampleSheet): bool = 
  # 检查是否有重复的Index
  var indexSet: HashSet[string]
  for k, v in this.sheetTab:
    let tag = &"""{v.getOrDefault("Lane", "")}{v["index"]}{v["index2"]}"""
    if not indexSet.contains(tag):
      indexSet.incl(tag)
    else:
      return true
  return false


when isMainModule:
  var s: SampleSheet
  s.parse_samplesheet(paramStr(1))
  echo $s.sheetTab
  echo $s.sheetHead
  if not s.dup_index():
    echo "yeah, no dup"
  else:
    echo "sigh, a dup"