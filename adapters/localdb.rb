# encoding: UTF-8
require 'cgi'
require 'digest/md5'
require 'lz4-ruby'

class LocalDB
	@@errors = nil
	def self.errors
		@errors
	end
	def self.write db, obj_name, value
		begin
			self.ensure_file_path(db, obj_name)
			File.open(self.file_name(db, obj_name),'wb') do |f|
				f.flock(File::LOCK_EX)
				d = LZ4::compress(value)
				f.write(d)
			end
		rescue Errno::ENOENT
			LocalDB.create db
			retry
		end
		return true
	rescue Exception => ex
		@@errors = ex.message
		return false
	end

	def self.read db, obj_name
		data = nil
		fn = self.file_name(db, obj_name)
		if File.exists?(fn)
			data = File.open(fn, 'rb').read
			data = LZ4::uncompress(data).force_encoding(Encoding::UTF_8)
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
		`rm -rf #{dd}/*`
	end

	def self.count db
		dd = self.data_dir(db)
		Dir.glob("#{dd}/*/*").count
	end

	protected

	def self.data_dir db
  	"./data/#{db}"
	end

	def self.file_name db, obj_name
		hex = Digest::MD5.hexdigest(obj_name)
		"#{self.data_dir(db)}/#{hex[0..2]}/#{hex[3..31]}"
	end

	def self.ensure_file_path db, obj_name
		hex = Digest::MD5.hexdigest(obj_name)
		partial_dir = "#{self.data_dir(db)}/#{hex[0..2]}"
		unless File.exists?(partial_dir)
			Dir::mkdir(partial_dir)
		end
	end
end
