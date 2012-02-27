(function() {
  var __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; },
    __slice = Array.prototype.slice;

  define(function() {
    var Scheme;
    Scheme = (function() {

      function Scheme() {}

      Scheme.prototype.scheme_type = function() {
        return this.scheme_type;
      };

      Scheme.prototype.is_scheme = true;

      return Scheme;

    })();
    return {
      TextLine: (function(_super) {

        __extends(_Class, _super);

        function _Class() {
          var fields;
          fields = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
          this.fields = fields;
          this.scheme_type = "TextLine";
          if (this.fields.length === 0) this.fields = ["line"];
        }

        _Class.prototype.to_java = function() {
          return Cascading.Factory.TextLine(this.fields);
        };

        return _Class;

      })(Scheme)
    };
  });

}).call(this);
