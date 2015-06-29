# doc-count-udf
A MarkLogic User Defined aggregation Function for accumulating elem/attr frequencies per uri

## Compile

Run `make` on the appropriate target (or equivalent) system. You cannot compile on MacOS and run on CentOS for instance!

## Install

Run the following in QConsole (database not important):

    xquery version "1.0-ml";

    import module namespace plugin = "http://marklogic.com/extension/plugin"
      at "MarkLogic/plugin/plugin.xqy";
  
    plugin:install-from-zip("grtjn",
      xdmp:document-get("/path/to/grtjn/doc-count-udf/doc-count.zip")/node())

## Use

For example:

    cts:aggregate(
      "grtjn/doc-count", 
      "doc-count", 
      (
        cts:uri-reference(),
        cts:element-attribute-reference(xs:QName("file"), xs:QName("size"))
      )
    )

This function returns a map:map with uri as keys, and sum of frequencies per uri as values.
