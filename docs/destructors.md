# Destructors and Lazy objects

VQL generally prefers to pass around the query lazy objects. This is
because generally a query will not retrieve all the results that can
be gathered from a plugin, so there is no point doing the extra work
of calculating the results when the SELECT query will eliminate them
anyway.

While lazy objects are preferred, they also introduce lifetime
considerations around maintaining the requirements for the calculation
valid. Since the whole point of lazy evaluating is that evaluation
might occur in the future, we need to maintain any requirements alive
long enough for this to happen.

## Example - parsing a complicated file

Consider a plugin in the following query

SELECT X FROM plugin()

Say the plugin needs to open several files, parse a number of
artifacts from them, and produce many rows. When would it make sense
for the files to be closed?

One approach is to add a scope destructor which will close each file.
However, this approach causes the files to not be closed until **all**
the rows have been delivered, and materialized.

Emitting lazy results also complicates matters because closing the
file on scope destruction may be too late...

## The naive approach - precalculate

One approach is to extract the needed data and simply close the file
in the plugin before pushing it into the output chan. The problem with
this approach is that we end up calculating much more than we might
need to because the query may eliminate a lot of the required work.

For example suppose we can calculate the properties X, Y and Z by
parsing the file, then the plugin can simply:

```golang
fd := Open(....)

output_chan <- ordereddict.NewDict().
  Set("X", CalculateX(fd)).
  Set("Y", CalculateY(fd)).
  Set("Z", CalculateZ(fd))

fd.Close()
```

However, our query above will just throw away Y and Z because only the
X property is neded: `SELECT X FROM plugin()`.

How can we produce lazy objects?

The following methods are used to create lazy result sets - i.e. they
will only be calculated when the query needs them. This makes it much
faster but now there is a problem with the lifetime of the open file.

## Lazy approach - only calculate needed properties

One way to produce lazy results is to return a LazyRow:

```golang
fd := Open(....)

row := NewLazyRow(ctx).
  AddColumn("X", func(ctx, scope) Any {
    return CalculateX(fd))
  }).AddColumn("Y", func(ctx, scope) Any {
   return CalculateY(fd))
  }).AddColumn("Z", func(ctx, scope) Any {
   return CalculateZ(fd))
  })

output_chan <- row
```

This solves the problem of evaluating Y and Z unnecessarily, but now
it is not clear when we should close the file? We must do so only
*after* the lazy function is evaulated but we do not know when that
will be.

If we immediately close the file after pushing the lazy row to the
output_chan, then when the callback is evaluated later on, the file
will be already closed and CalculateX(fd) will fail!

### Adding a scope destructor

Adding a scope destructor appears to work for the simple
`SELECT X FROM plugin()` query but will fail for this one:

```sql
LET results <= SELECT X FROM plugin()

SELECT * FROM foreach(
  row=results,
  query={
    SELECT * FROM plugin2(filename=X)
})
```

This is because the `results` variable is already materialized and
evaluated on a subscope which is closed when done (i.e. the file will
be closed after the `LET` query but and by the time we are accessing
the lazy functions).

### Possible solutions - LRU

This problem is difficult to solve based on scope alone - one approach
is to maintain an LRU cache of file objects which may be closed (and
reopened) at any time on demand.

This approach works well when it is possible to recreate the
conditions for the lazy evaluation in the future. For example with
open files, it is always possible to reopen the file - its just a bit
slower.

Maintaining an LRU of objects that can re-open the file by themselves
helps because even if the object is evicted from the LRU it can easily
reopen the file and rejoin the LRU.

This solution does not work well with things like temp files - for
them eviction from the LRU means the file is deleted and so it can not
be recreated:

```sql
SELECT * FROM foreach(
  row={ SELECT tempfile() AS TempFile FROM plugin() },
  query={
    SELECT * FROM plugin(filename=TempFile)
})
```

In the above example the tempfile() plugin will produce a temporary
file and add a scope destructor to remove it. It will emit the name of
this tempfile in the TempFile column.

However, the local scope ends in the row query and the file may be
removed before the next query can use it. A time based LRU might help
here.

Another possibility is to implement reference counting for the file -
this needs all users of the file ot be able to reference count usage
of the file and it is a much more complex task.


## Struct based Lazy rows

Instead of using the LazyRow object as mentioned above, it is also
possible for the plugin to release a custom struct which may be
convenient in some cases.

For example define a type that provides Getter methods with no
parameters (VQL will treat those as properties and call the method on
access):

```golang
type FileParser struct {
   fd io.ReadCloser
}

func (self *FileParser) X() Any {
  return CalculateX(self.fd)
}

func (self *FileParser) Y() Any {
  return CalculateY(self.fd)
}

func (self *FileParser) Z() Any {
  return CalculateZ(self.fd)
}

fd, err := Open( ...)
output_chan <- &FileParser{fd: fd}
```

The query `SELECT X FROM plugin()` will now call the X() methods but
not Y() or Z() avoiding the extra work.

This is particularly important for parsers who can spend a lot of time
and cpu parsing files - even if the query does not actually need that
information. Writing parsers in a lazy fasion helps make them more
efficient.

## Alternative 2 - ordereddict based rows

As an alternative to the previous method, it is also possible for a
plugin to forward an ordereddict based row with callables as values

```golang
output_chan <- ordereddict.NewDict().
       Set("X", func() {CalculateX(fd)}). <-- lazy functions evaulated on access.
       Set("Y", func() {CalculateY(fd)}).
       Set("Z", func() {CalculateZ(fd)})
```
