Pod::Spec.new do |spec|
  spec.name         = "ThetaDB"
  spec.version      = "0.0.2"

  spec.summary      = "A lightweight, embedded key-value database for mobile clients."
  spec.description  = <<-DESC
                      ThetaDB is suitable for use on mobile clients with "High-Read, Low-Write" demands.
                      DESC

  spec.homepage     = "https://github.com/TangentW/ThetaDB"
  spec.license      = { :type => "MIT", :file => "LICENSE" }
  spec.author       = { "Tangent" => "tangent_w@outlook.com" }

  spec.platform     = :ios, "13.0"
  spec.source       = { :git => "https://github.com/TangentW/ThetaDB.git", :tag => "#{spec.version}" }
  spec.swift_version = '4.0'

  spec.subspec 'Core' do |ss|
    ss.source_files = "ios/ThetaDB/*.swift"
    ss.vendored_frameworks = "ios/ThetaDBFFI.xcframework"
  end

  spec.subspec 'Coding' do |ss|
    ss.source_files = "ios/ThetaDB+Coding/*.swift"
    ss.dependency "ThetaDB/Core"
  end
end
