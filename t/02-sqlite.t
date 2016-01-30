#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use DBI;
use DBD::SQLite;

use My::TableFetch;

my $file = shift || ":memory:";

my $dbh = DBI->connect("dbi:SQLite:dbname=$file", "", "", {RaiseError => 1});

$dbh->do( "CREATE TABLE foo ( foo_id INT PRIMARY KEY, bar_id INT );" );
$dbh->do( "CREATE TABLE bar ( bar_id INT PRIMARY KEY, baz_id INT );" );
$dbh->do( "CREATE TABLE baz ( baz_id INT PRIMARY KEY, foo_id INT );" );

foreach( 1..20 ) {
	$dbh->prepare( "INSERT INTO foo(foo_id, bar_id) VALUES (?,?)" )->execute( 2*$_, $_ );
	$dbh->prepare( "INSERT INTO foo(foo_id) VALUES (?)" )->execute(  2*$_-1 );
};
foreach( 1..20 ) {
	$dbh->prepare( "INSERT INTO bar(bar_id, baz_id) VALUES (?,?)" )->execute(  3*$_, $_ );
	$dbh->prepare( "INSERT INTO bar(bar_id) VALUES (?)" )->execute(  3*$_-1 );
	$dbh->prepare( "INSERT INTO bar(bar_id) VALUES (?)" )->execute(  3*$_-2 );
};
foreach( 1..20 ) {
	$dbh->prepare( "INSERT INTO baz(baz_id, foo_id) VALUES (?,?)" )->execute(  5*$_, $_ );
	$dbh->prepare( "INSERT INTO baz(baz_id) VALUES (?)" )->execute(  5*$_-1 );
	$dbh->prepare( "INSERT INTO baz(baz_id) VALUES (?)" )->execute(  5*$_-2 );
	$dbh->prepare( "INSERT INTO baz(baz_id) VALUES (?)" )->execute(  5*$_-3 );
	$dbh->prepare( "INSERT INTO baz(baz_id) VALUES (?)" )->execute(  5*$_-4 );
};

my $tf = My::TableFetch->new( dbh => $dbh );
while (<DATA>) {
	$tf->read_rule( $_ );
};

# note explain $tf;

$tf->do_fetch( { table => "foo", key => "foo_id", value => 30 } );

note explain [ $tf->get_data ];
is (scalar $tf->get_data, 4, "4 datapoints saved" );


my $dbh2 = DBI->connect("dbi:SQLite:dbname=:memory:", "", "", {RaiseError => 1});
$dbh2->do( "CREATE TABLE foo ( foo_id INT PRIMARY KEY, bar_id INT );" );
$dbh2->do( "CREATE TABLE bar ( bar_id INT PRIMARY KEY, baz_id INT );" );
$dbh2->do( "CREATE TABLE baz ( baz_id INT PRIMARY KEY, foo_id INT );" );
$tf->{dbh} = $dbh2;

my @rollback = map { $tf->ifsert_row(%$_) } $tf->get_data;

my $sth = $dbh2->prepare( "SELECT f.foo_id, b.bar_id, z.baz_id, z.foo_id AS foo2
	FROM foo f JOIN bar b USING(bar_id) JOIN baz z USING(baz_id)
	WHERE f.foo_id = ?" );

$sth->execute(30);

is_deeply( $sth->fetchrow_arrayref, [30, 15, 5, 1], "SELECT as expected");
is( $sth->fetchrow_arrayref, undef, "Only one row");

foreach (@rollback) {
	$dbh2->do($_);
};

foreach my $table (qw(foo bar baz)) {
	my $sth_all = $dbh2->prepare("SELECT * FROM $table;");
	$sth_all->execute();
	my $count = 0;
	while (my $row = $sth_all->fetchrow_arrayref) {
		diag "Extra row in $table: ", explain $row;
		$count++;
	};

	ok (!$count, "Table $table now empty - rollback");
};

done_testing;

__DATA__
#this is a comment

KEY foo foo_id
KEY bar bar_id
KEY baz baz_id

LINK foo:bar_id bar
LINK bar:baz_id baz:baz_id
LINK baz:foo_id foo
