(function() {
  var __indexOf = Array.prototype.indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  define(function() {
    var moduleKeywords;
    moduleKeywords = ['extended', 'included'];
    return {
      Module: (function() {

        function _Class() {}

        _Class.extend = function(obj) {
          var key, value, _ref;
          for (key in obj) {
            value = obj[key];
            if (__indexOf.call(moduleKeywords, key) < 0) this[key] = value;
          }
          if ((_ref = obj.extended) != null) _ref.apply(this);
          return this;
        };

        _Class.include = function(obj) {
          var key, value, _ref;
          for (key in obj) {
            value = obj[key];
            if (__indexOf.call(moduleKeywords, key) < 0) {
              this.prototype[key] = value;
            }
          }
          if ((_ref = obj.included) != null) _ref.apply(this);
          return this;
        };

        return _Class;

      })()
    };
  });

}).call(this);
