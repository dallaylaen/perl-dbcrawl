#!/usr/bin/env perl

use strict;
use warnings;

use DBI;
use JSON::XS;
use Getopt::Long;

use FindBin qw($Bin);
use File::Basename qw(dirname);
use lib dirname($Bin)."/lib";
use My::TableFetch;

# config: KEY table field,... (?)
# config: LINK table:field table[:field]
# need tables in memory to avoid circles
# need "ifsert" - skip if present in target DB
# dump structure: { table => ..., id => "key", data => { ... } }

@ARGV or usage();

my ($database);
my %fname;
GetOptions (
	"d|database=s" => \$database,
	"s|save=s"     => \$fname{out},
	"l|load=s"     => \$fname{in},
	"r|rollback=s" => \$fname{rollback},
	"f|rules=s"    => \$fname{rules},
	"h|help"       => \&usage,
) or die "Bad options";

sub usage {
	print <<"USAGE";
Usage: $0 [options]
To be continued...
USAGE
	exit 0;
};

$fname{out} xor $fname{in}
	or die "Exactly one of -l or -s must be specified";

my $dbparams = str2dbi( $database );
# TODO read password if needed
my $tf = My::TableFetch->new(dbh => db_connect($dbparams));

if ( $fname{out} ) {
	# TODO - = STDOUT
	open (my $out, ">", $fname{out})
		or die "Failed t open(w) $fname{out}: $!";
	read_rules( $tf, $fname{rules} );

	my @init = map {
		/(\w+):(\w+)=(.*)/ or die "Bad table:key=value spec";
		{
			table => $1,
			key   => $2,
			value => $3,
		};
	} @ARGV;

	$tf->do_fetch( @init );
	foreach my $record( $tf->get_data ) {
		print $out encode_json( $record )."\n"
			or die "Failed to write to $fname{out}: $!";
	};
	close $out or die "Failed to sync $fname{out}: $!";
} elsif ( $fname{in} ) {
	open ( my $fd_js, "<", $fname{in} )
		or die "Cannot open(r) $fname{in}: $!";

	my $fd_roll;
	if (defined $fname{rollback}) {
		open ($fd_roll, ">", $fname{rollback})
			or die "Cannot open(w) $fname{rollback}: $!";
	};

	$tf->dbh->begin_work;

	my $result = eval {
		if ($fd_roll) {
			print $fd_roll "BEGIN WORK;\n"
				or die "Failed to write to $fname{rollback}: $!";
		};
		while (<$fd_js>) {
			chomp;
			my $data = decode_json($_);
			my $rb = $tf->ifsert_row(%$data);
			if ($fd_roll) {
				print $fd_roll "$rb\n"
					or die "Failed to write to $fname{rollback}: $!";
			};
		};
		if ($fd_roll) {
			print $fd_roll "COMMIT;\n"
				or die "Failed to write to $fname{rollback}: $!";
			close $fd_roll
				or die "Failed to close $fname{rollback}: $!";
		};
	};
	$result ? $tf->dbh->commit : $tf->dbh->rollback;
	if (!$result) {
		die $@ || "Error during inserts, rolling back";
	};
};


sub str2dbi {
	my $str = shift;

	$str or die "No db connect spec given";
	$str =~ m#^(\w+):(.*)@([\w\.\-]+)(?::(\d+))?/(\w+)$#
		or die "Wrong DB connect format";
	my $port = $4 || 3306;

	return {
		db      => "dbi:mysql:host=$3:port=$port:dbname=$5",
		user    => $1,
		pass    => $2,
	};
};

sub db_connect {
	my $param = shift;

	return DBI->connect( $param->{db}, $param->{user}, $param->{pass}
		, { RaiseError => 1 } );
};

# Or should it be in module?
sub read_rules {
	my ($tf, $fname) = @_;

	open (my $fd, "<", $fname)
		or die "Failed to open(r) $fname: $!";

	my $i;
	while (<$fd>) {
		$i++;
		$tf->read_rule($_, "$fname:$i");
	};

	return $tf;
};

