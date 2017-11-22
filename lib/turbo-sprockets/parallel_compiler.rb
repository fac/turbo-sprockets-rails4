require 'benchmark'
require 'parallel'
require 'json'

module TurboSprockets
  class ParallelCompiler
    attr_reader :manifest

    def initialize(manifest)
      @manifest = manifest
    end

    def compile(*args)
      logger.warn "Precompiling with #{worker_count} workers"

      time = Benchmark.measure do
        results = compile_in_parallel(find_precompile_paths(*args))
        write_manifest(results)
        write_compile_stats(results)
      end

      logger.info "Completed precompiling assets (#{time.real.round(2)}s)"
    end

    private

    def write_manifest(results)
      File.write(manifest.filename, results.slice("files", "assets").to_json)
    end

    def write_compile_stats(results)
      File.open(historical_stats_path, "w") do |f|
        f.write results.slice("compile_time").to_json
      end
    end

    # Internal - Schedule assets paths into N (worker_count) buckets evenly
    #            according to historical stats before pre-compile them in
    #            parallel in order to achive best perf
    #
    # Returns an array of array
    def assign_buckets(paths)
      buckets = worker_count.times.map { [] }
      bucket_weights = worker_count.times.map { 0 }

      if File.exist?(historical_stats_path)
        logger.info("Schedule assets using #{historical_stats_path}...")
        historical_stats = JSON.parse(File.read(historical_stats_path))["compile_time"]

        weighted_paths = paths.map do |path|
          [path, (historical_stats[path] || 0)]
        end

        desc_weighted_paths = weighted_paths.sort { |x, y|  y[1] <=> x[1] }

        desc_weighted_paths.each do |weighted_path|
          path, weight = weighted_path[0], weighted_path[1]

          # bucket with the min weight
          i = bucket_weights.index(bucket_weights.min)

          buckets[i] << path

          bucket_weights[i] += weight
        end
      else
        logger.warn("#{historical_stats_path} doesn't exist, scheduling paths into bucket randomly")
        paths.each_with_index do |path, i|
          buckets[i % worker_count].push path
        end
      end

      buckets
    end

    def compile_in_parallel(paths)
      buckets = assign_buckets(paths)

      flatten_precomp_results(
        Parallel.map(buckets, in_processes: worker_count) do |paths|
          manifest.compile_without_parallelism([paths])

          { 'files' => {}, 'assets' => {}, 'compile_time' => {} }.tap do |data|
            manifest.find([paths]) do |asset|
              next if File.exist?(asset.digest_path) # don't recompile
              logger.info("Writing #{asset.digest_path}")

              data['files'][asset.digest_path] = properties_for(asset)
              data['assets'][asset.logical_path] = asset.digest_path
              data['compile_time'][asset.logical_path] = asset.compile_time

              if alias_logical_path = manifest.class.compute_alias_logical_path(asset.logical_path)
                data['assets'][alias_logical_path] = asset.digest_path
                data['compile_time'][alias_logical_path] = asset.compile_time
              end
            end
          end
        end
      )
    end

    def flatten_precomp_results(results)
      results.each_with_object({}) do |result, ret|
        result.each_pair do |key, data|
          (ret[key] ||= {}).merge!(data)
        end
      end
    end

    def find_precompile_paths(*args)
      paths, filters = args.flatten.partition do |pre|
        manifest.class.simple_logical_path?(pre)
      end

      filters = filters.map do |filter|
        manifest.class.compile_match_filter(filter)
      end

      environment.logical_paths.each do |logical_path, filename|
        if filters.any? { |f| f.call(logical_path, filename) }
          paths << filename
        end
      end

      paths
    end

    def properties_for(asset)
      {
        'logical_path' => asset.logical_path,
        'mtime'        => asset.mtime.iso8601,
        'size'         => asset.bytesize,
        'digest'       => asset.hexdigest,
      }
    end

    def worker_count
      TurboSprockets.configuration.precompiler.worker_count
    end

    def environment
      manifest.environment
    end

    def logger
      TurboSprockets.configuration.precompiler.logger
    end

    def historical_stats_path
      Rails.root.join("public/.assets_precompile_stats")
    end
  end
end
