require "bundler/gem_tasks"
require "everyleaf/embulk_helper/tasks"

Everyleaf::EmbulkHelper::Tasks.install({
  gemspec: "./embulk-input-sfdc.gemspec",
  github_name: "treasure-data/embulk-input-sfdc",
})

task default: :test

desc "Run tests"
task :test do
  ruby("test/run-test.rb", "--use-color=yes")
end
