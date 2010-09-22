# -*- ruby -*-

require 'rubygems'
require 'hoe'
require "rake/extensiontask"

Hoe.spec 'freetds' do
  developer('Erik Bryn', 'erik.bryn@gmail.com')
  self.readme_file   = 'README'
  self.history_file  = 'CHANGELOG'
  self.extra_rdoc_files  = FileList['*.rdoc']
  self.extra_dev_deps << ['rake-compiler', '>= 0']
  self.spec_extras = { :extensions => ["ext/freetds/extconf.rb"] }

  Rake::ExtensionTask.new('freetds', spec) do |ext|
    ext.lib_dir = File.join('lib', 'freetds')
  end
end

Rake::Task[:test].prerequisites << :compile
# vim: syntax=ruby
