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

my $tf = My::TableFetch->new();


sub str2dbi {
	my $str = shift;


	$str =~ m#^(\w+):(.*)@([\w\.\-]+)(?::(\d+))/\w+$#
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


