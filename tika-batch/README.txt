This is a dev version of the tika-batch module (tracked as a subtask under TIKA-1302).

The initial version will batch process one directory and write to an output directory.

See https://wiki.apache.org/tika/TikaBatchOverview for an overview.

I've included an example driver script, log4j config files and
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
output
    word.doc.json
    excel.xls.json

Once this code is ready to go, I'll integrate it into tika-app so
that the batch-mode will be built into the app.