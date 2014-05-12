# encoding: UTF-8
require 'cgi'
require 'digest/md5'
require 'lz4-ruby'
class LocalDB
	def self.write db, obj_name, value
		# Not sure if you have to utf8 it before you save it or not
		# value = value.encode('UTF-8', invalid: :replace, undef: :replace)
		# puts "input format: #{value.encoding}"
		begin
			File.open(self.file_name(db, obj_name), "w:UTF-8") do |f|
				f.flock(File::LOCK_EX)
				# d = LZ4::compress(value)
				# puts "input lz4 result: #{d.encoding}"
				f.write(value)
			end
		rescue Errno::ENOENT
			LocalDB.create db
			File.open(self.file_name(db, obj_name), "w:UTF-8") do |f|
				f.flock(File::LOCK_EX)
				# LZ4::compress(value)
				f.write(value)
			end
		end
		return true
	rescue Errno::ENOENT
		return false
	end

	def self.read db, obj_name
		data = nil
		fn = self.file_name(db, obj_name)
		if File.exists?(fn)
			data = File.read(fn, encoding: 'UTF-8')
			# puts "output encoding from file: #{d.encoding}"
    	# data = LZ4::uncompress(d)
			# puts "output format: #{data.encoding}"
		end
		data
	end

	def self.create db
		dd = self.data_dir db
		unless File.exists?(dd)
			Dir::mkdir(dd)
		end
	end

	def self.destroy db
		dd = self.data_dir db
		if File.exists?(dd)
			`rm -rf #{dd}`
		end
	end


	def self.clear db
		dd = self.data_dir(db)
		`rm #{dd}/*`
    # Dir.foreach(dd) {|f| fn = File.join(dd, f); File.delete(fn) if f != '.' && f != '..'}
	end

	def self.list
		dbs = []
		Dir.foreach(self.data_dir(nil)){ |f| dbs << f if f != '.' && f != '..' }
		dbs
	end


	protected
		def self.data_dir db
	  	"./data/#{db}"
  	end

  	def self.file_name db, obj_name
			hex = Digest::MD5.hexdigest(obj_name)
  		fn = "#{self.data_dir(db)}/#{hex[0..2]}"
			unless File.exists?(fn)
				Dir::mkdir(fn)
			end
			"#{fn}/#{hex[3..31]}"
  	end
end
