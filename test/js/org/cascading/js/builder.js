(function() {
  var listing_fields, module, modules, paths;

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

  listing_fields = ["listing_id", "state", "user_id", "title", "description", "creation_tsz", "ending_tsz", "original_creation_tsz", "last_modified_tsz", "price", "currency_code", "quantity", "tags", "materials", "section_id", "featured_rank", "views", "image_listing_id", "state_tsz", "last_modified_tsz_epoch", "saturation", "brightness", "is_black_and_white"];

  require(paths, function(builder, schemes) {
    var with_test_flow;
    with_test_flow = function(f) {
      var cascade;
      return cascade = builder.cascade(function($) {
        return $.flow('word_counter', function() {
          return f($);
        });
      });
    };
    return describe("job builder", function() {
      it("should set outgoing scope on tap", function() {
        return with_test_flow(function($) {
          var tap;
          tap = $.source('input', $.tap("listings.txt", new schemes.TextLine()));
          return expect(tap.outgoing).toEqual(["line"]);
        });
      });
      return it("should set incoming scope on assembly", function() {
        return with_test_flow(function($) {
          var a1, a2;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          $.source('input_2', $.tap("listings.txt", new schemes.TextLine("line_2")));
          console.log($.assembly);
          a1 = $.assembly('input', function() {});
          a2 = $.assembly('input_2', function() {});
          expect(a1.incoming).toEqual(["offset", "line_1"]);
          return expect(a2.incoming).toEqual(["line_2"]);
        });
      });
    });
  });

}).call(this);
