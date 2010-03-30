#!/usr/bin/ruby
#____________________________________________________________________ 
# File: file_receiver.rb
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-03-02 15:44:23+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------

# Open a file to write content to:
timestamp=Time.now.tv_sec
received_files=File::open("/home/isdc/iftsops/backup_scheduler/received_files/revno_#{timestamp}",'w')

STDIN.each do |line|
  received_files.print(line)
end

received_files.close()
