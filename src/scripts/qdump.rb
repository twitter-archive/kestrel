#!/usr/bin/env ruby

CMD_ADD = "\000"
CMD_REMOVE = "\001"
CMD_ADDX = "\002"

f = File.open(ARGV[0], "r")
while !f.eof
  b = f.read(1)
  if b == CMD_ADD
    len = f.read(4).unpack("I")[0]
    data = f.read(len)
    expire = data.unpack("I")[0]
    puts "add #{len} expire #{expire}"
  elsif b == CMD_REMOVE
    puts "remove"
  elsif b == CMD_ADDX
    len = f.read(4).unpack("I")[0]
    data = f.read(len)
    add_time, expire = data.unpack("QQ")
    puts "addx #{len} expire #{expire}"
  end
end
