(function() {
  var module, modules, paths;

  modules = ["builder", "schemes"];

  paths = (function() {
    var _i, _len, _results;
    _results = [];
    for (_i = 0, _len = modules.length; _i < _len; _i++) {
      module = modules[_i];
      _results.push("../../../../../src/js/org/cascading/js/" + module);
    }
    return _results;
  })();

  require(paths, function(builder, schemes) {
    return describe("job builder", function() {
      return it("should work", function() {
        var c;
        c = builder.cascade(function($) {
          return $.flow('word_counter', function() {
            $.source('input', $.tap("test.txt", new schemes.TextLine()));
            $.assembly('input', function() {
              $.generator(["line"], ["word"], function(tuple, emitter) {
                var word, _i, _len, _ref, _results;
                _ref = tuple.line.match(/\S+/g);
                _results = [];
                for (_i = 0, _len = _ref.length; _i < _len; _i++) {
                  word = _ref[_i];
                  _results.push(emitter({
                    word: word
                  }));
                }
                return _results;
              });
              return $.insert({
                capitalized: function(tuple) {
                  return tuple.word.toUpperCase();
                }
              });
            });
            return $.sink('input', $.tap("output", new schemes.TextLine()));
          });
        });
        return c.to_java();
      });
    });
  });

}).call(this);