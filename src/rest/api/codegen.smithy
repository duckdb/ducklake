$version: "2"

// Traits that tell scripts/generate_rest_api.py how the shapes of the API map to C++. They do not change the JSON of
// the API.
namespace ducklake.rest.codegen

/// The headers that declare the C++ classes that shapes are bound to.
@trait(selector: "service")
list cppIncludes {
    member: String
}

/// Binds a structure or enum to an existing C++ class or enum, whose members are read and written directly.
@trait(selector: ":is(structure, enum)")
string cppClass

/// The member of the C++ class that holds a member, if it has another name.
@trait(selector: "member")
string cppMember

/// The C++ type of a member of a generated structure, if it is not the default C++ type of its shape.
@trait(selector: "member")
string cppType

/// An optional member that is stored as a value and a flag of the C++ class that says whether it is set.
@trait(selector: "member")
string hasMember

/// An optional member that is stored as a value and a flag of the C++ class that says whether it is unset.
@trait(selector: "member")
string nullMember

/// A list of entries that is stored as a C++ map: the key of an entry is its `keyField`, the value its `valueField` or
/// the fields of the mixin `value`.
@trait(selector: "member :test(> list > member > structure)")
structure cppMap {
    @required
    keyField: String

    valueField: String

    @idRef(failWhenMissing: true, selector: "structure")
    value: String
}

/// The C++ expression (of `value`) that says whether a value of a bound structure is null.
@trait(selector: "structure")
string nullIf

/// The endpoint has no prefix of a catalog in its URL: it is how a client finds the prefix.
@trait(selector: "operation")
structure noPrefix {}

/// The JSON conversion of the structure is written by hand (src/rest/ducklake_rest_serialization.cpp).
@trait(selector: "structure")
structure customSerialization {}

/// The member is not part of the JSON document: it is sent as an Arrow IPC stream in a part of a multipart message that
/// has the name.
@trait(selector: "member")
string arrowOnly

/// The member of the structure that holds the types of the columns of `rows` - a list of types, or a member of a list
/// of structures (e.g. `columns.type`).
@trait(selector: "member")
string rowTypes

/// How a simple shape is encoded.
@trait(selector: "simpleType")
enum wireType {
    UINT64 = "uint64"
    TIMESTAMP = "timestamp"
    TEXT = "text"
    LOGICAL_TYPE = "logical_type"
    ROWS = "rows"
    PATH = "path"
}

/// The sections of the reference documentation.
@trait(selector: "service")
list docSections {
    member: DocSection
}

structure DocSection {
    @required
    title: String

    /// The endpoints (`read`, `command`, `commit`) or the `types` of the section.
    @required
    kind: String

    documentation: String
}
