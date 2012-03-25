(function() {

  job(function($, _) {
    return $.flow('word count', function() {
      var assembly;
      $.source('input', $.tap("words3.txt", $.text_line_scheme("offset", "word")));
      assembly = $.assembly('input', function() {
        var count, last_key;
        last_key = null;
        count = 0;
        return $.foreach_group(["word"], {
          add: ["count"],
          types: {
            count: "int"
          }
        }, (function(tuple) {
          return count = 0;
        }), (function(tuple, writer) {
          return count += 1;
        }), (function(tuple, writer) {
          return writer({
            count: count
          });
        }));
      });
      return $.sink('input', $.tap("output", $.text_line_scheme("word", "count")));
    });
  });

}).call(this);
