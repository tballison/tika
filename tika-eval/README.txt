This is a very first cut at an eval module that will compare
two directories of output from Tika.  Run one version of tika with
RecursiveParserWrapper dumping json-ized List<Metadata> in one directory
and another version of Tika with the same format in another directory.

This module will then run some basic comparisons on a per file basis.
The goal is to identify regressions or compare two text/metadata extractors.

Much more work remains.

This issue is tracked as a subproject to TIKA-1302.