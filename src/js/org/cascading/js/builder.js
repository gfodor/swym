(function() {
  var __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  define(["./components", "./schemes", "./helpers", "./common"], function(components, schemes, helpers, common) {
    var Builder;
    Builder = (function(_super) {

      __extends(Builder, _super);

      Builder.include(helpers.Components);

      Builder.include(helpers.EachPipes);

      Builder.include(helpers.EveryPipes);

      function Builder() {
        this.components = {};
        this.assembly_stack = [];
      }

      Builder.prototype.register = function(component) {
        if (this.components[component.name] != null) {
          throw new Error("Duplicate component " + component.name);
        }
        return this.components[component.name] = component;
      };

      Builder.prototype.get = function(name) {
        return this.components[name];
      };

      return Builder;

    })(common.Module);
    return {
      cascade: function(f) {
        var builder;
        builder = new Builder();
        return builder.cascade(f);
      }
    };
  });

}).call(this);
