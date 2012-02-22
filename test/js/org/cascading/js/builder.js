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
        return builder.cascade(function($) {
          return $.flow('word_counter', function() {
            $.source('input', $.tap("test.txt", new schemes.TextLine()));
            return console.log("in flow");
          });
        });
      });
    });
  });

}).call(this);
