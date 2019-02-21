module TurboSprockets
  class Railtie < ::Rails::Railtie
    initializer 'turbo-sprockets' do
      unless Sprockets::Manifest.method_defined?(:compile_with_parallelism)
        Sprockets::Manifest.class_eval do
          def compile_with_parallelism(*args)
            if TurboSprockets.configuration.precompiler.enabled?
              TurboSprockets::ParallelCompiler.new(self).compile(*args)
            else
              compile_without_parallelism(*args)
            end
          end

          alias_method :compile_without_parallelism, :compile
          alias_method :compile, :compile_with_parallelism
        end
      end
    end

    config.after_initialize do
      if ::TurboSprockets.configuration.preloader.enabled?
        # actually do the preloading
        TurboSprockets::ParallelPreloader.preload!

        # for some reason parallel operations may cause activerecord to
        # disconnect
        ::ActiveRecord::Base.clear_active_connections! if const_defined?(:ActiveRecord)
      end
    end
  end
end
