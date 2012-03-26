(function() {

  job(function($, _) {
    return $.flow('word count', function() {
      var assembly;
      $.source('input', $.tap("test.txt", $.text_line_scheme("line")));
      assembly = $.assembly('input', function() {
        var count;
        $.map({
          add: {
            word: "string"
          },
          remove: ["line"]
        }, function(tuple, writer) {
          var word, _i, _len, _ref, _results;
          _ref = tuple.line.match(/\S+/g);
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            word = _ref[_i];
            _results.push(writer({
              word: word
            }));
          }
          return _results;
        });
        count = 0;
        return $.foreach_group(["word"], {
          add: {
            count: "int"
          }
        }, function(tuple) {
          return count = 0;
        }, function(tuple, writer) {
          return count += 1;
        }, function(tuple, writer) {
          return writer({
            count: count
          });
        });
      });
      return $.sink('input', $.tap("output", $.text_line_scheme("word", "count")));
    });
  });

}).call(this);
