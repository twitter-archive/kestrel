Gem::Specification.new do |s|
  s.name = "kcluster"
  s.version = File.open("VERSION").read.chomp

  s.authors = ["Robey Pointer"]
  s.email = "robeypointer@gmail.com"
  s.homepage = "http://robey.github.com/kestrel"
  s.summary = "Kestrel cluster management tool"
  s.description = """\
A handy little CLI tool for browsing quick stats across your kestrel cluster.
Kcluster collects stats from each machine in a cluster, then summarizes them
into one table.
"""

  s.files = Dir.glob("lib/**/*.rb") + [ 'bin/kcluster' ]
  s.require_paths = [ "lib" ]
  s.executables = [ "kcluster" ]

  s.add_dependency 'json'#, '~> 1.0.0'
end
