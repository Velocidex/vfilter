# The veloci-filter (vfilter) library implements a generic SQL like query language.

Overview::

There are many applications in which it is useful to provide a flexible
query language for the end user. Velocifilter has the following design
goals:

 - It should be generic and easily adaptable to be used by any project.
 - It should be fast and efficient.

An example makes the use case very clear. Suppose you are writing an
archiving application. Most archiving tools require a list of files to
be archived (e.g. on the command line).

You launch your tool and a user requests a new flag that allows them to
specify the files using a glob expression. For example, a user might
wish to only select the files ending with the ".go" extension. While on
a unix system one might use shell expansion to support this, on other
operating systems shell expansion may not work (e.g. on windows).

You then add the ability to specify a glob expression directly to your
tool (suppose you add the flag --glob). A short while later, a user
requires filtering the files to archive by their size - suppose they
want to only archive a file smaller than a certain size. You studiously
add another set of flags (e.g. --size with a special syntax for greater
than or less than semantics).

Now a user wishes to be able to combine these conditions logically (e.g.
all files with ".go" extension newer than 5 days and smaller than 5kb).

Clearly this approach is limited, if we wanted to support every possible
use case, our tool would add many flags with a complex syntax making it
harder for our users. One approach is to simply rely on the unix "find"
tool (with its many obscure flags) to support the file selection
problem. This is not ideal either since the find tool may not be present
on the system (E.g. on Windows) or may have varying syntax. It may also
not support every possible condition the user may have in mind (e.g.
files containing a RegExp or files not present in the archive).

There has to be a better way. You wish to provide your users with a
powerful and flexible way to specify which files to archive, but we do
not want to write complicated logic and make our tool more complex to
use.

This is where velocifilter comes in. By using the library we can provide
a single flag where the user may specify a flexible VQL query (Velocidex
Query Language - a simplified SQL dialect) allowing the user to specify
arbirarily complex filter expressions. For example:

    SELECT file from glob(pattern=["*.go", "*.py"]) where file.Size < 5000
    and file.Mtime < now() - "5 days"

Not only does VQL allow for complex logical operators, but it is also
efficient and optimized automatically. For example, consider the
following query:

    SELECT file from glob(pattern="*") where grep(file=file,
    pattern="foobar") and file.Size < 5k

The grep() function will open the file and search it for the pattern. If
the file is large, this might take a long time. However velocifilter
will automatically abort the grep() function if the file size is larger
than 5k bytes. Velocifilter correctly handles such cancellations
automatically in order to reduce query evaluation latency.

## Protocols - supporting custom types::

Velocifilter uses a plugin system to allow clients to define how their
own custom types behave within the VQL evaluator.

Note that this is necessary because Go does not allow an external
package to add an interface to an existing type without creating a new
type which embeds it. Clients who need to handle the original third
party types must have a way to attach new protocols to existing types
defined outside their own codebase. Velocifilter achieves this by
implementing a registration systen in the Scope{} object.

For example, consider a client of the library wishing to pass custom
types in queries:

        type Foo struct {
           ...
           bar Bar
        }  Where both Foo and Bar are defined and produced by some other library

which our client uses. Suppose our client wishes to allow addition of
Foo objects. We would therefore need to implement the AddProtocol
interface on Foo structs. Since Foo structs are defined externally we
can not simply add a new method to Foo struct (we could embed Foo struct
in a new struct, but then we would also need to wrap the bar field to
produce an extended Bar. This is typically impractical and not
maintainable for heavily nested complex structs). We define a FooAdder{}
object which implements the Addition protocol on behalf of the Foo
object.

```
          // This is an object which implements addition between two Foo objects.
          type FooAdder struct{}

          // This method will be run to see if this implementation is
          // applicable. We only want to run when we add two Foo objects together.
          func (self FooAdder) Applicable(a Any, b Any) bool {
                _, a_ok := a.(Foo)
                _, b_ok := b.(Foo)
                return a_ok && b_ok
          }

          // Actually implement the addition between two Foo objects.
          func (self FooAdder) Add(scope *Scope, a Any, b Any) Any {
            ... return new object (does not have to be Foo{}).
          }
```

Now clients can add this protocol to the scope before evaluating a
query:

```
    scope := NewScope().AddProtocolImpl(FooAdder{})
```