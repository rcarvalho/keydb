require './adapters/localdb'
class TwoToOne
  def self.convert
    Dir.glob("#{dd}/*").each do |basedir|
      Dir.glob("#{basedir}/*") do |subdir|
        Dir.glob("#{subdir}/*") do |file|
          nfn = subdir.split('/').last + file.split('/').last
          File.rename(file, "#{basedir}/#{nfn}")
        end
        Dir.delete(subdir)
      end
    end
  end
end
