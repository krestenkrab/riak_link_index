

<h1>Link Indexing</h1>

This module allows you to create simple secondary indexes
in Riak based on Riaks' link model.  The basic idea is thus:

Assume we model people and companies as separate buckets:

    /riak/person/Name
    /riak/company/Name

When you store a `/riak/person/Kresten` object, you describe the
employment relation by including this link in the Kresten object:

    X-Riak-Link: </riak/company/Trifork>; riaktag="idx@employs"

The magic is that `riak_link_index` will then automatically add a link
in the opposite direction; from `Trifork` to `Kresten`, and that link
will have tag `employs`.  The tag needs to bve start with `idx@` for
`riak_link_index` to recognize it.

Whenever you update a person object, you can pass in new (or multiple)
such links, and the old reverse links will automatically be
deleted/updated as appropriate.

This module also allows you to install an `index_hook`, which can be
used to extract links from your objects.  Index hooks can be written in
both JavaScript for Erlang.


Installation
============

To install, you need to make the `ebin` directory containing
`riak_link_index.beam` accessible to your Riak install.  You can do that
by adding a line like this to riaks `etc/vm.args`

<pre>-pz /Path/to/riak_function_contrib/other/riak_link_index/ebin</pre>

If you're an Erlang wiz there are other ways, but that should work.


Next, you configure a bucket to support indexing.  This involves two things:

1. Install a set of commit hooks (indexing needs both a pre- and a
   post-commit hook).

2. (optionally) configure a function to extract index information
   from your bucket data.  We'll do that later, and start out with
   the easy version.

If your bucket is name `person`, it could be done thus:

<pre>
prompt$ cat > bucket_props.json
{
  "precommit"  : [{"mod": "riak_link_index", "fun": "precommit"}],
  "postcommit" : [{"mod": "riak_link_index", "fun": "postcommit"}]
}
^D
prompt$ curl -X PUT --data @bucket_props.json \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8091/riak/person
</pre>

There you go: you're ready for some action.

Explicit Indexing
=================


The simple indexer now works for the `person` bucket, by interpreting
links on `/riak/person/XXX` objects that have tags starting with
`idx@`.  The special `idx@` prefix is recognized by the indexer, and
it will create and maintain a link in the opposite direction, tagged
with whatever comes after the `idx@` prefix.

Let's say we add me:

    curl -X PUT \
      -H 'X-Riak-Link: </riak/company/Trifork>; riaktag="idx@employs"' \
      -H 'Content-Type: application/json' \
      --data '{ "name": "Kresten Krab Thorup", "employer":"Trifork" }' \
      http://127.0.0.1:8091/riak/people/kresten

As this gets written to Riak, the indexer will then
create an object by the name of `/riak/company/Trifork`,
which has a link pointing back to me:

    curl -v -X GET http://127.0.0.1:8091/riak/company/Trifork
    < 200 OK
    < X-Riak-Links: </riak/people/Kresten>; riaktag="employs"
    < Content-Length: 0

If there was already an object at `/company/Trifork`, then the indexer
would leave the contents alone, but still add the reverse link.  If no
such object existed, then it would be created with empty contents.

Link Walking
============

The beauty of this is that you can now do link-walk queries to find
your stuff.  For instance, this link query should give you a list of
people employed at Trifork.  Lucky them :-)

    curl http://localhost:8091/riak/company/Trifork/_,_,employs

Using a link_index hook
=======================

You can also install an index hook as a bucket property, which designates
a function that can be used to decide which index records to create.  This way
you can keep the index creation on the server side; and also more easily
generate some more indexes.

You install the index hook the same way you install a pre-commit hook; and the
hook can be written in either Erlang or JavaScript, just like precommits.

Assume you have this installed inside `priv/` in your riak setup

    // Return list of [Bucket,Key] that will link to me
    function employmentIndexing(metaData, contents) {
      personData = JSON.parse(contents);
      if(personData.employer) {
        return [ ['company', personData.employer] ];
      } else {
        reuturn [];
      }
    }

To install it as an indexer, you need to get install it as a bucket
property in the person bucket.  You can have more indexes, so it's a
list of functions.  Link-Index hooks can also be erlang functions.

    prompt$ cat > bucket_props.json
    {
      "link_index"  : [{"name": "employmentIndexing", "tag": "employs"}],
    }
    ^D
    prompt$ curl -X PUT --data @bucket_props.json \
      -H 'Content-Type: application/json' \
      http://127.0.0.1:8091/riak/person

Now, we can add objects to the person bucket *without* having to put
the `idx@employs` link on the object.  The index hook will do it for
you.  Happy you!


Maintenance
===========

The indexer will also handle delete/update of your records as
appropriate, and should work fine with `allow_mult` buckets too.


Status
======

The code is at present completely untested, and does not work yet on
0.14 (release) branch, only on the `master`, because it needs to
communicate stuff from the precommit hook to the postcommit hook, and
right now it just passes that in the pdict, but as I said, that only
wortks on the current develepoment master.


