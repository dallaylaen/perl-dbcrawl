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
$tf->add_table ( table => "foo", key => "foo_id" );
$tf->add_table ( table => "bar", key => "bar_id" );
$tf->add_table ( table => "baz", key => "baz_id" );

$tf->add_link( from_table => "foo", from_key => "bar_id", to_table => "bar" );
$tf->add_link( from_table => "bar", from_key => "baz_id", to_table => "baz" );
$tf->add_link( from_table => "baz", from_key => "foo_id", to_table => "foo" );

$tf->do_fetch( { table => "foo", key => "foo_id", value => 30 } );


note explain [ $tf->get_data ];
