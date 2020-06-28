namespace cpp pisco.thrift
namespace php Pisco.Thrift

const i32 DEFAULT_LIMIT = 10
typedef map<string,bool> attrs

struct Item {
  1: required i32 id,
  2: required i32 time,
  3: required string value,
  4: required bool deleted = false,
  5: optional attrs attributes
}

struct Result {
  1: required list<i32> ids,
  2: optional i32 total = 0
}

struct Query {
  1: required string pattern,
  2: optional i32 limit = DEFAULT_LIMIT,
  3: optional bool total_required = false
}

struct AdvancedQuery {
  1: optional set<string> include_patterns,
  2: optional set<string> exclude_patterns,
  3: optional attrs with_attributes,
  4: optional i32 limit = DEFAULT_LIMIT,
  5: optional bool total_required = false,
  6: optional bool include_deleted = false
}

service Search {
  Result lookup(1: Query query),
  Result lookupAdvanced(1: AdvancedQuery query),
  bool add(1: Item item),
  bool replace(1: Item item),
  bool remove(1: i32 id)
}