define ["underscore"], (_) ->
  # Returns sorted keys
  skeys: (obj) ->
    ks = _.keys(obj)
    ks.sort()
    ks

  type_idx_map:
    int: 0
    long: 1
    bool: 2
    double: 3
    date: 4
    string: 5

  rtype_idx_map: ["int", "long", "bool", "double", "date", "string"]
