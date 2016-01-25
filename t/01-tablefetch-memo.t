#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use JSON::XS;

use My::TableFetch;

my $tf = My::TableFetch->new;

$tf->add_table( table => "foo", key => "foo_id" );
$tf->add_table( table => "bar", key => "bar_id" );
$tf->add_link( from_table => "foo", to_table => "bar", from_key => "bar_id" );

is_deeply( [ $tf->add_data(table => "foo", data => { foo_id => 1 }) ]
	, [], "no linked tables, no more fetch - no more fetch" );
is_deeply( [ $tf->add_data(table => "foo", data => { foo_id => 2, bar_id => 2 }) ]
	, [{ table => "bar", key => "bar_id", value => 2 }], "1 link found" );
is_deeply( [ $tf->add_data(table => "foo", data => { foo_id => 2, bar_id => 2 }) ]
	, [], "second time add no more" );

my $json_exp = <<'JSON';
{"table":"foo","key":"foo_id","data":{"foo_id":1}}
{"table":"foo","key":"foo_id","data":{"bar_id":2,"foo_id":2}}
JSON

is_deeply(
	[ sort map { encode_json($_) } $tf->get_data ],
	[ sort map { encode_json(decode_json($_)) } split /\n/, $json_exp ],
	"returned get_data as expected");

done_testing;
