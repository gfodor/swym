(function() {
  var listing_fields;

  listing_fields = ["listing_id", "state", "user_id", "title", "description", "creation_tsz", "ending_tsz", "original_creation_tsz", "last_modified_tsz", "price", "currency_code", "quantity", "tags", "materials", "section_id", "featured_rank", "views", "image_listing_id", "state_tsz", "last_modified_tsz_epoch", "saturation", "brightness", "is_black_and_white"];

  require({
    baseUrl: "lib/js"
  }, ["cascading/builder", "cascading/schemes"], function(builder, schemes) {
    var expect_bad_flow, with_test_flow;
    with_test_flow = function(f) {
      var cascade;
      return cascade = builder.cascade(function($) {
        return $.flow('word_counter', function() {
          return f($);
        });
      });
    };
    expect_bad_flow = function(msg, f) {
      return expect(function() {
        return with_test_flow(f);
      }).toThrow(new Error(msg));
    };
    return describe("job builder", function() {
      it("should set outgoing scope on tap", function() {
        with_test_flow(function($) {
          var tap;
          tap = $.source('input', $.tap("listings.txt", new schemes.TextLine()));
          return expect(tap.outgoing).toEqual(["line"]);
        });
        return with_test_flow(function($) {
          var tap;
          tap = $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line")));
          return expect(tap.outgoing).toEqual(["offset", "line"]);
        });
      });
      it("should fail if trying to tap bad source name", function() {
        return expect_bad_flow("Unknown source bad_input for assembly", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          a1 = $.assembly('bad_input', function() {});
          return $.sink('input', $.tap("output", new schemes.TextLine()));
        });
      });
      it("should set incoming scope on assembly", function() {
        return with_test_flow(function($) {
          var a1, a2;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          $.source('input_2', $.tap("listings.txt", new schemes.TextLine("line_2")));
          a1 = $.assembly('input', function() {});
          a2 = $.assembly('input_2', function() {});
          expect(a1.head_pipe.incoming).toEqual(["offset", "line_1"]);
          expect(a1.head_pipe.outgoing).toEqual(["offset", "line_1"]);
          expect(a2.head_pipe.incoming).toEqual(["line_2"]);
          return expect(a2.head_pipe.incoming).toEqual(["line_2"]);
        });
      });
      it("should fail if no sinks", function() {});
      it("should fail if no assembly for sink", function() {});
      it("should fail if duplicate assembly", function() {});
      it("should fail if duplicate source", function() {});
      it("should fail if duplicate sink", function() {});
      it("should fail if any unbound sinks", function() {});
      it("should fail if any unbound sources", function() {});
      it("should fail if TextLine doesn't have 0, 1, 2 fields", function() {});
      return it("should fail if textline doesn't have 1 or 2 fields", function() {});
    });
  });

}).call(this);
