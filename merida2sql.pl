#perl2exe_info FileVersion=6.0000
#perl2exe_info ProductName=merida2sql
#perl2exe_info ProductVersion=6.0.0
#perl2exe_info Copyright=Oslo universitetetssykehus-OSS MTV

#perl2exe_include overloading
#perl2exe_include encoding
#perl2exe_include PerlIO
#perl2exe_exclude master
#perl2exe_exclude Encode::CN
#perl2exe_exclude Encode::JP
#perl2exe_exclude Encode::KR
#perl2exe_exclude Encode::TW

#########################################
#
#  merida2sql.pl - Dump Merida to SQL
#  OuS Medisinsk Teknologisk Avdeling, TK
#  v1.0 - 2015.08 - Initial version
#  v2.0 - 2015.10 - Calculate field lengths for decimals as well (default of (18,0) is not usable)
#  v3.0 - 2015.11 - Explicit ISO-8859-1 support during file operations and sql bulk import
#  v4.0 - 2015.11 - Process last lines in import tables
#  v5.0 - 2015.11 - Mark line feeds in comments to be processed later
#  v6.0 - 2016.11 - More documentation and verbose output

use strict;
use warnings;
use Getopt::Long;
use File::Path qw(make_path);
use File::Spec;
use Text::ParseWords;

my $NAME = "merida2sql";
my $VERSION= "6";

my $CRLF = '_xCRLFx_';
my $FSEP = '|xox|';

# Progress to SQL Server data type mappings
# http://documentation.progress.com/output/ua/OpenEdge_latest/index.html#page/dmsql/data-types.html
my $field_mapping = {

	"character" => "varchar", # Dump files are ISO8859-1
	"integer" => "integer",
	"int64" => "bigint",
	"logical" => "bit",
	"decimal" => "decimal",
	"date" => "date",
	"datetime-tz" => "datetimeoffset",
	"datetime" => "datetime2"
	
};

my $table_definition = {};
my $definition = undef;
my $dump_directory = undef;
my $target_directory = undef;
my $sqldatabase = undef;
my $verbose = 0;

GetOptions (
	"definition=s" => \$definition,
	"dumpdirectory=s" => \$dump_directory,
	"targetdirectory=s" => \$target_directory,
	"database=s" => \$sqldatabase,
	"verbose" => \$verbose++,
	"usage" => sub { PrintUsage(0) },
	) or PrintUsage(1);

defined $definition && defined $dump_directory && defined $target_directory && defined $sqldatabase 
	or PrintUsage(2); 

open (DEF, "< :encoding(Latin1)", $definition) or die "cannot open < $definition: $!";

print "Loading definition file $definition ...\n" if $verbose;

my @datadef = (); 
while (<DEF>)
{
	next if /^$/;
	chomp;
	
	# Bugfix - replace semicolon in description field by comma :-)
	s/Type of attachment; 0 - file, 1 -graphic image/Type of attachment, 0 - file, 1 -graphic image/;
	s/Book type; 1 = book, 2 = regular publication/Book type, 1 = book, 2 = regular publication/;
	s/Password for external access to MERIDA; type = 1/Password for external access to MERIDA, type = 1/;
	s/Type of employment; 1 = permanent, 2 = temporary/Type of employment, 1 = permanent, 2 = temporary/;
	
	# check lines like "localisation;Textual description of localisation;character;x(50) * 3"
	if ($_ =~ /(.+);(.*);(.+);(.+)/)
	{
		# Check for extra semicolons
		die "Field line '$_' has unprocessed semicolons.\n" if ($1 =~/;/) || ($2 =~/;/) || ($3 =~/;/) || ($4 =~/;/);
		my ($lname, $ldesc, $ltype, $tdesc) = ($1, $2, $3, $4);
		
		$tdesc =~ s/\s+//g; # remove spaces
		
		my ($lmul) = ($tdesc =~ /\*(\d+)$/);
			
		if (defined $lmul) # multiple definition indicator is found
		{

			$tdesc =~ s/\*\d+$//;
			print "Multiple field ($lmul) - $lname, $tdesc\n" if $verbose > 1;
			for (1 .. $lmul)
			{
				push @datadef,"$lname" . "___$_;$ldesc;$ltype;$tdesc"; 
			}		
			next;
			
		} else {
			push @datadef,"$lname;$ldesc;$ltype;$tdesc";		
		}
		
	} else {
		push @datadef, $_;
	}
	
}

close DEF;

my $current_table = undef;

foreach (@datadef)
{
	# Get table name
	# string example: "Table: account;Dump name: account"
	if ($_ =~ /Table: (.+);Dump name: (.+)/)
	{
		$current_table = $1;
		$table_definition->{$current_table}{'dumpname'} = $2;
		$table_definition->{$current_table}{'fields'} = [];
		next;
	}
	
	# string example: "element_id;Relation id (from unique_id), object=account-element;integer;>>>>>>9"	
	if ($_ =~ /(.+);(.*);(.+);(.+)/)
	{						
		push @{$table_definition->{$current_table}{'fields'}}, { 'name' => $1, 'description' => $2, 'type' => $3, 'typedescriptor' => $4 };	
	}
}

print "Generating SQL Create table statements\n" if $verbose;
-d $target_directory || make_path($target_directory);

open INITSQL, "> :encoding(Latin1)", "$target_directory/init.sql" or die $!;
open BULKSQL, "> :encoding(Latin1)", "$target_directory/bulk.sql" or die $!;

# Bulk import requires the full path
my $abstarget = File::Spec->rel2abs($target_directory);

print BULKSQL "
/*
NB! Replace all occurences of '$abstarget' by the full path of the current directory on your machine
*/

";

print INITSQL "
USE master;
GO
CREATE DATABASE [$sqldatabase];
GO
use [$sqldatabase];
GO
";

# Creating SQL statements for table creation
foreach my $table_name (sort keys %{$table_definition})
{

	my $dumpname = "$dump_directory/" . $table_definition->{$table_name}{dumpname} . ".d";
	if (-z $dumpname)
	{
		print "$dumpname - empty, skipping table creation\n" if $verbose > 1;
		next;
	}
	
	print INITSQL "/* Table definition $table_name */\n\n";
	
	print INITSQL "CREATE TABLE [$table_name] (\n";

	my $n = 0;
	
	my $ntablefield = scalar @{$table_definition->{$table_name}{'fields'}};
	print "$table_name - ($ntablefield defined fields <=> " if $verbose > 1;
	
	my @fieldcount = TableScan($target_directory, 1, $table_name, $dumpname, $ntablefield);
	print scalar @fieldcount . " scanned fields)\n" if $verbose > 1;
	
	($ntablefield == scalar @fieldcount) || die "Field counts don't match. Stop.\n";
	
	foreach (@{$table_definition->{$table_name}{'fields'}})	
	{
		my $lname = $_->{name};
		my $ltype = lc $_->{type};		
		my $ldesc = lc $_->{typedescriptor};
		my $fdesc = $_->{description};
		
		if (exists $field_mapping->{$ltype})
		{
			print INITSQL "\t[" . $lname . "] " . $field_mapping->{$ltype}
		} else {
			die "No field mapping for " . $ltype . ". Stop.\n";
		}
		
		if ($ltype eq 'character')
		{	
			$fieldcount[$n] ||= 2; # min length 2
			$fieldcount[$n] = 'max' if $fieldcount[$n] >= 8000;
			
			print INITSQL "(" . $fieldcount[$n]  . ")";
			
		} elsif ($ltype eq 'decimal') # examples: >,>>>,>>>,>>9 ZZZZZ9.999 >>>9.99 9.99 ->>,>>9.99
		{	
			print INITSQL "(25,5)";
		}

		print INITSQL ",";		
		print INITSQL " -- $fdesc (#$n)\n";
		
		$n++;
		
	}
	
	print INITSQL ");\n";
}

close INITSQL;
close BULKSQL;

# Create a small README file
open README, ">", "$target_directory/README.txt" or die $!;
print README "

This directory contains all SQL files you need to create an SQL database ($sqldatabase) corresponding to the Merida dump.

Steps to follow:

o Run 'init.sql' to create the database and tables

>>> SQLCMD -S Server\\Instance -i init.sql

o Update 'bulk.sql' and replace all occurences '$abstarget' by the full path of the current directory

o Run 'bulk.sql' to import all contents

>>> SQLCMD -S Server\\Instance -i bulk.sql

o Replace '_xCRLFx_' by a line shift allover the database

";
close README;

################
#
#  Functions
#
sub TableScan
{
	my ($sqldir, $printlength, $table, $dumpfile, $fieldcount) = @_;

	my $count = 0;
	
	open (DUMP, "< :encoding(Latin1)", $dumpfile) or die "cannot open < $dumpfile: $!";

	my @strings_found = ();

	my $line = undef;
	my @field = ();
	
	my @fieldlen = (0) x $fieldcount;

	if (defined $sqldir)
	{
		-d $sqldir || make_path($sqldir);
		open BULKDATA, "> :encoding(Latin1)", "$sqldir/bulk_" . $table . ".txt" or die $!;
	}

	while (<DUMP>) {

#		print "$.\n" if defined $verbose and ($. % 10000 == 0);
	
		chomp;
		s/\r/$CRLF/g; # Keep line shifts
		s/\\/\\\\/g; 
		s/'/''/g; 
	
		if (/\d+ / ) # Main assumption - records start with a number
		{
			if (defined $line)
			{
				if (ProcessLine($line, $fieldcount, \@fieldlen))
				{
					$line = $_;
					$count++;
					next;
				}			
			}
		}

		if (defined $line)
		{
			$line .= $CRLF . $_; # Keep line shift
		} else {
			$line = $_;
		}
	}
	
	# process the last import line if exists
	(ProcessLine($line, $fieldcount, \@fieldlen) && $count++) if defined $line;
	
	close DUMP;
	close BULKDATA;

	if (defined $sqldir)
	{		
		print BULKSQL "

use [$sqldatabase];
go

PRINT '**** Table $table'

BULK INSERT [dbo].[$table]
    FROM '$abstarget\\bulk_$table.txt'
    WITH
    (
    FIELDTERMINATOR = '$FSEP',
    ERRORFILE = '$abstarget\\dbo.$table.errors.txt',
	CODEPAGE = '28591'
    )

";
	}

	print "$table - $count entries\n" if $verbose;
	
	return ($printlength ? @fieldlen : undef);
}

sub ProcessLine()
{

	my ($line, $lfieldcount, $pfieldlen) = @_;

	my @field = quotewords(" ", 0, $line);

	for (0 .. ($lfieldcount - 1)) # update max field lengths
	{
		if (defined $field[$_])
		{
			$pfieldlen->[$_] = length $field[$_] if $pfieldlen->[$_] < length $field[$_];
		}
	}

	if (scalar @field == $lfieldcount)
	{	
		map {s/^\?$//} @field; # Remove ? (undefined?)
		map {s/(\d\d)\/(\d\d)\/(\d\d\d\d)/$3$2$1/} @field; # date conversion - YYYYMMDD
		map {s/^yes$/1/} @field; # yes -> 1
		map {s/^no$/0/} @field; # no -> 0
		map {s/^([-\d]*),(\d+)$/$1\.$2/} @field; # replace decimal ',' to '.'
								
#		$sqldir && (print INSSQL "INSERT INTO dbo.$table VALUES (" . join(",", map("'$_'", @field)) .");\n");					
		print BULKDATA join("$FSEP", map("$_", @field)) ."\n";
				
		return 1; # returns true if progress export line is processed for export
	}
	
	return 0; # returns false if progress export line is incomplete
}

sub PrintUsage
{
	print "
	
$NAME $VERSION - Dump Merida to SQL

Usage:
    merida2sql --definition Merida database definition file
    --dumpdirectory Merida dump directory --targetdirectory SQL dump
    directory --sqldatabase database name [--verbose] [--help]

Options:
    --definition Merida database definition file
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

    --dumpdirectory Merida dump directory
        Required. A Merida dump directory contains dump files generated by
        Merida. Steps to create a Merida dump:

         o Start Merida
         o Select menu 'System->Database->Merida->Dump'
         o A dialog box for a directory select will appear
         o Select a proper path and create a directory within it
         o Click OK

        NB! Dependent on the database size, a dump operation may take some
        time. You will see some progress message on the screen.

    --targetdirectory SQL dump directory
        Required. This is the target directory in where all SQL equivalents
        of a Merida dump will be generated. A README file will also be
        generated. Upon a successful completion, you can simply zip that
        directory and send to the vendor. The directory should be empty and
        will be created automatically if it doesn't exist.

    --database sql database name
        Required. Use that option to specify a name for the SQL database
        which will be created.

    --verbose
        Optional. Produces progress messages during running of the program.
        Turned off by default.

    --help
        Produces help message.

";

	exit shift;
}

__END__

=head1 NAME

B<merida2sql> - Dump Merida to SQL 

=head1 SYNOPSIS

B<merida2sql> B<--definition> I<Merida database definition file> B<--dumpdirectory> I<Merida dump directory> B<--targetdirectory> I<SQL dump directory> B<--database> I<SQL database name> [B<--verbose>] [B<--help>]

=head1 DESCRIPTION

B<merida2sql> creates an SQL equivalent of a Merida dump directory. The result directory can be packed and sent to the vendor for migration from Merida. 

=head1 OPTIONS

=over 4 

=item B<--definition> I<Merida database definition file>

Required. A Merida database definition file describes the database structure used by Merida and will be used to interpret the corresponding Merida dump files properly. Steps to create a definition file:

 o Start Merida
 o Select menu 'Rapporter->Database->Menu'
 o Select database from the list (normally called 'merida' and selected by default)
 o Click 'Data definition' button
 o Select 'Fil', specify a file location and select 'Semikolon' as format
 o Click OK

=item B<--dumpdirectory> I<Merida dump directory>

Required. A Merida dump directory contains dump files generated by Merida. Steps to create a Merida dump:

 o Start Merida
 o Select menu 'System->Database->Merida->Dump'
 o A dialog box for a directory select will appear
 o Select a proper path and create a directory within it
 o Click OK

NB! Dependent on the database size, a dump operation may take some time. You will see some progress message on the screen.

=item B<--targetdirectory> I<full path of the SQL dump directory>

Required. This is the target directory in where all SQL equivalents of a Merida dump will be generated. A README file will also be generated. Upon a successful completion, you can simply zip that directory and send to the vendor. The directory should be empty and will be created automatically if it doesn't exist.

=item B<--database> I<SQL database name>

Required. Use that option to specify a name for the SQL database which will be created.

=item B<--verbose>

Optional. Produces progress messages during running of the program. Turned off by default.

=item B<--help>

Produces help message.

=back

=head1 EXAMPLE

 merida2sql.pl --def d:\temp\merida\merida-db.def --dump d:\temp\merida --target d:\temp\sql --database test --verbose

Loads Merida database definitions from I<d:\temp\merida\merida-db.def>, scans all Merida dump files in the I<d:\temp\merida> according to the database definition, generates SQL command files in I<d:\temp\sql> to create the database I<test> with corresponding tables, and populating them by bulk updates. Creates also a small B<README.txt> file to describe steps to be taken at the vendor side.

=head1 EXIT VALUES

 0 Normal termination
 1 Invalid argument
 2 Missing argument

=head1 AUTHOR

Tevfik Karagulle, Oslo universitetssykehus, OSS MTV

=head1 COPYRIGHT

Oslo universitetssykehus, 2016

=head1 VERSION

Version 6 November 2016

=head1 CHANGELOG

=over 4

=item version 6, nov 2016

 - More documentation and verbose output
 - Create a README file
 - Introduce target directory as an SQL variable

=item version 5, nov 2015

 - Mark line feeds in comments to be processed later

=item version 4, nov 2015

 - Process last lines in import tables

=item version 3, nov 2015

 - Explicit ISO-8859-1 support during file operations and sql bulk import

=item version 2, oct 2015

 - Calculate field lengths for decimals as well (default of (18,0) is not usable)

=item version 1, aug 2015

 - Initial release

=back

=cut
