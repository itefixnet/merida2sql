# merida2sql - Dump Merida to SQL

## NAME
   merida2sql - Dump Merida to SQL

## SYNOPSIS
   merida2sql --definition *Merida database definition file* --dumpdirectory
   *Merida dump directory* --targetdirectory *SQL dump directory* --database
   *SQL database name* [--verbose] [--help]

## DESCRIPTION
   merida2sql creates an SQL equivalent of a Merida dump directory. The
   result directory can be packed and sent to the vendor for migration from
   Merida.

## OPTIONS
   --definition *Merida database definition file*
       Required. A Merida database definition file describes the database
       structure used by Merida and will be used to interpret the
       corresponding Merida dump files properly. Steps to create a
       definition file:

        o Start Merida
        o Select menu 'Rapporter->Database->Menu'
        o Select database from the list (normally called 'merida' and selected by default)
        o Click 'Data definition' button
        o Select 'Fil', specify a file location and select 'Semikolon' as format
        o Click OK

   --dumpdirectory *Merida dump directory*
       Required. A Merida dump directory contains dump files generated by
       Merida. Steps to create a Merida dump:

        o Start Merida
        o Select menu 'System->Database->Merida->Dump'
        o A dialog box for a directory select will appear
        o Select a proper path and create a directory within it
        o Click OK

       NB! Dependent on the database size, a dump operation may take some
       time. You will see some progress message on the screen.

   --targetdirectory *full path of the SQL dump directory*
       Required. This is the target directory in where all SQL equivalents
       of a Merida dump will be generated. A README file will also be
       generated. Upon a successful completion, you can simply zip that
       directory and send to the vendor. The directory should be empty and
       will be created automatically if it doesn't exist.

   --database *SQL database name*
       Required. Use that option to specify a name for the SQL database
       which will be created.

   --verbose
       Optional. Produces progress messages during running of the program.
       Turned off by default.

   --help
       Produces help message.

## EXAMPLE
    merida2sql.pl --def d:\temp\merida\merida-db.def --dump d:\temp\merida --target d:\temp\sql --database test --verbose

   Loads Merida database definitions from *d:\temp\merida\merida-db.def*,
   scans all Merida dump files in the *d:\temp\merida* according to the
   database definition, generates SQL command files in *d:\temp\sql* to
   create the database *test* with corresponding tables, and populating them
   by bulk updates. Creates also a small README.txt file to describe steps
   to be taken at the vendor side.

## EXIT VALUES
    0 Normal termination
    1 Invalid argument
    2 Missing argument

## AUTHOR
   Tevfik Karagulle, Oslo universitetssykehus, OSS MTV

## COPYRIGHT
   Oslo universitetssykehus, 2016

## VERSION
   Version 6 November 2016

## CHANGELOG
   version 6, nov 2016
        - More documentation and verbose output
        - Create a README file
        - Introduce target directory as an SQL variable

   version 5, nov 2015
        - Mark line feeds in comments to be processed later

   version 4, nov 2015
        - Process last lines in import tables

   version 3, nov 2015
        - Explicit ISO-8859-1 support during file operations and sql bulk import

   version 2, oct 2015
        - Calculate field lengths for decimals as well (default of (18,0) is not usable)

   version 1, aug 2015
        - Initial release

