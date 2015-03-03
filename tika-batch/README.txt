This is an experimental version of the tika-batch module (tracked as a subtask under TIKA-1302).

At this point, expect code in flux.  There are no guarantees of backwards
compatibility in the configuration or behavior of this module until Tika 1.9 or Tika 2.0.

This initial version will batch process one directory and write to an output directory.

See https://wiki.apache.org/tika/TikaBatchOverview for an overview.

This can be run as part of tika-app, just specify an -inputDir and an -outputDir on the commandline.

Or, you can run this as a wrapper around custom jars.

We've included an example driver script, log4j config files and
a batch-config file under src/main/examples.

Put the log4j files and jar files in bin.  Put the tika-batch-config file
at the same level as the .sh and run.



bin/
    log4j.xml
    log4j_driver.xml
    *.jar
input/
    word.doc
    excel.xls
output/
    word.doc.json
    excel.xls.json

