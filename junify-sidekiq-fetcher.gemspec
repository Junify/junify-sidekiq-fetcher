Gem::Specification.new do |s|
  s.name          = 'junify-sidekiq-fetcher'
  s.version       = '0.9.1.junify.1'
  s.authors       = ['TEA', 'GitLab', 'Junify']
  s.license       = 'LGPL-3.0'
  s.homepage      = 'https://github.com/Junify/junify-sidekiq-fetcher'
  s.summary       = 'Reliable fetch extension for Sidekiq'
  s.description   = 'Redis reliable queue pattern implemented in Sidekiq'
  s.require_paths = ['lib']
  s.files         = `git ls-files`.split($\)
  s.test_files    = []
  s.add_dependency 'sidekiq', '~> 6.5.12'
  s.add_runtime_dependency 'json', '>= 2.5'
  s.add_runtime_dependency 'base64', '>= 0.1', '< 1'
end
