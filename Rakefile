require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "sequel-jdbc-hxtt-adapter"
    gem.summary = %Q{Sequel adapter for working with MS Access files using JRuby and HXTT's pure JDBC Access driver}
    gem.description = %Q{Sequel adapter for working with MS Access files using JRuby and HXTT's pure JDBC Access driver.  Note: HXTT driver is not supplied but you can obtain a trial version or purchase it at http://www.hxtt.com/}
    gem.email = "casey.colin@gmail.com"
    gem.homepage = "http://github.com/colincasey/sequel-jdbc-hxtt-adapter"
    gem.authors = ["Colin Casey"]
    gem.add_development_dependency "thoughtbot-shoulda"
    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: sudo gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/*_test.rb'
  test.verbose = true
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/*_test.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  if File.exist?('VERSION')
    version = File.read('VERSION')
  else
    version = ""
  end

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "sequel-jdbc-hxtt-adapter #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
