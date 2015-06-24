require "bundler/gem_tasks"
require "json"

task default: :test

desc "Run tests"
task :test do
  ruby("test/run-test.rb", "--use-color=yes")
end

namespace :release do
  desc "Add header of now version release to ChangeLog and bump up version"
  task :prepare do
    # TODO: fix versioning

    root_dir = Pathname.new(File.expand_path("../", __FILE__))
    changelog_file = root_dir.join("CHANGELOG.md")

    system("git fetch origin")

    # detect merged PR
    old_version = "0.0.2" # FIXME
    pr_numbers = `git log v#{old_version}..origin/master --oneline`.scan(/#[0-9]+/)

    if !$?.success? || pr_numbers.empty?
      puts "Detecting PR failed. Please confirm if any PR were merged after the latest release."
      exit(false)
    end

    # Generate new version
    major, minor, patch = old_version.split(".").map(&:to_i)
    new_version = "#{major}.#{minor}.#{patch + 1}"

    # Update ChangeLog
    pr_descriptions = pr_numbers.map do |number|
      body = open("https://api.github.com/repos/treasure-data/embulk-input-sfdc/issues/#{number.gsub("#", "")}").read
      payload = JSON.parse(body)
      "* [] #{payload["title"]} [#{number}](https://github.com/treasure-data/embulk-input-sfdc/pull/#{number.gsub('#', '')})"
    end.join("\n")

    new_changelog = <<-HEADER
## #{new_version} - #{Time.now.strftime("%Y-%m-%d")}
#{pr_descriptions}

#{changelog_file.read.chomp}
HEADER

    File.open(changelog_file, "w") {|f| f.write(new_changelog) }

    # Update version.rb
    version_file = root_dir.join("./lib/embulk/input/sfdc/version.rb")
    old_content = version_file.read
    File.open(version_file, "w") do |f|
      f.write old_content.gsub(old_version, new_version)
    end

    # Update Gemfile.lock
    system("bundle install")

    puts "ChangeLog, version and Gemfile.lock were updated. New version is #{new_version}."
  end
end
