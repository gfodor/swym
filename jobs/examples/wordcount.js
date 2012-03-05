(function() {

  job(function($, _) {
    return $.flow('word count', function() {
      var assembly;
      $.source('input', $.tap("wordlist.txt", $.text_line_scheme("offset", "word")));
      assembly = $.assembly('input', function() {
        var count, last_key;
        last_key = null;
        count = 0;
        return $.foreach_group(["word"], {
          add: ["count"]
        }, (function(tuple, writer) {
          if (last_key !== tuple.word) {
            last_key = tuple.word;
            count = 0;
          }
          return count += 1;
        }), (function(writer) {
          return writer({
            count: count
          });
        }));
      });
      return $.sink('input', $.tap("output", $.text_line_scheme("word", "count")));
    });
  });

}).call(this);
