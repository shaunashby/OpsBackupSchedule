#!/usr/bin/ruby
#--
# File: schedule-backups.rb
# Author: Shaun ASHBY <Shaun.Ashby@unige.ch>
# Update: 2010-03-02 10:16:57+0100
# Revision: $Id$ 
#++
#
# This script is used to schedule backups of critical operations machines
# around the time of INTEGRAL perigee. This is to reduce load on these 
# machines during normal data-taking periods.
#
# The script is run from isdcifts machine as itfsops user upon reception
# of a new revno file, delivered using IFTS from MOC. The ifts_action_generic_untar
# script unpacks the received tar ball and triggers
#
#    cat ../inbox.tmp/revno | ssh iftsops@backuppc ~iftsops/backup_scheduler/schedule-backups.rb
#
# for any file having the name "revno". These are received from MOC typically every 2 or 3 days.
#
# This script reads the revno file contents from STDIN and creates a Revolution object 
# for each revolution in the future (determined by checking the current time against the 
# perigee time from the file). Existing queued backup jobs for each machine with start 
# times matching perigee times (+/- some offset) for revolutions contained in the new 
# file are removed.
#
# New jobs are queued for all machines requiring backup for every revolution. An offset
# can be applied to alter the start time of the backup relative to perigee time (slower 
# machines might need to start earlier, for example, so that most of the backup is 
# complete by the time that telemetry data resumes after perigee).
#
# Jobs are run using "at". A config file called "config.yml" must exist in the 
#

require 'yaml'

# Generic job queue class.
class Queue
  AT='/usr/bin/at'
  ATQ='/usr/bin/atq'
  ATRM='/usr/bin/atrm'
  
  # Class for queued job entries.
  class QueueEntry
    def initialize(jobid)
      @jobid=jobid
      @machine=nil
      @revnum=nil
      self.getinfo
    end
    
    # Get rev and machine info from the queued job.
    # This information is set via the environment variables 
    # <tt>REVNO</tt> and <tt>MACHINE</tt>
    # when  "at" queues the job and so is available inside the queued job script
    # which can be read using "at -c <jobid>" and parsing the output.
    def getinfo
      query_command=sprintf("%s -c %s",AT,@jobid)
      revnum_pattern=Regexp::compile('^REVNO=(\d+);\s*')
      machine_pattern=Regexp::compile('^MACHINE=(.*?);\s*')
      begin
        IO::popen(query_command) do |f|
          while line = f.gets
            line.chomp!
            if md = revnum_pattern.match(line)
              @revnum = md[1]
            elsif md = machine_pattern.match(line)
              @machine = md[1]
            end
          end
        end  
      rescue => err
        print($stderr,"ERROR trying to popen(#{query_command}): #{err}")
        exit(1)
      end      
    end

    # Job ID as returned by "atq"
    attr_reader :jobid
    # Machine pertaining to this entry
    attr_reader :machine
    # Revolution number
    attr_reader :revnum
    
    # Validity. An entry is only valid if both machine and revnum 
    # are set (there could be other jobs queued which we want to ignore).
    def valid?
      return !@machine.nil? && !@revnum.nil?
    end
    
    def to_s
      return sprintf("QueueEntry: %s %s %s",@jobid,@revnum,@machine)
    end
  end
  
  def initialize()
    @entries=Hash::new()
  end
  
  # Return the entries in the queue.
  attr_reader :entries
  
  # Populate the Queue object using "atq" to get the list of queued jobs.
  # For each entry the job ID is stored.
  def populate
    job_pattern=Regexp::compile('^(\d+)\s*')
    atq_command=sprintf("%s",ATQ)
    begin
      IO::popen(atq_command) do |f|
        while line = f.gets
          line.chomp!
          if id = job_pattern.match(line)
            entry = QueueEntry::new(id[1])
            if entry.valid?
              # Add to an entries hash where the key is the rev number and the value is
              # an array of QueueEntry objects. There are likely to be more than one 
              # entries per revolution (n_entries = n_machines).
              if !@entries.has_key?(entry.revnum)
                @entries[entry.revnum] = Array::new()                
              end
              @entries[entry.revnum] << entry
            end
          end
        end
      end      
    rescue => err
      print($stderr,"ERROR trying to popen(#{atq_command}): #{err}")
      exit(1)
    end
  end
  
  # Find a QueueEntry. Parameters are
  # [rev]     The revolution number
  # [machine] The machine for which the backup should be queued
  # The matching entries are returned in an array.
  def find(rev,machine)
    if @entries.has_key?(rev)
      found = Array::new
      @entries[rev].each do |e|
        if e.machine == machine
          found << e
        end
      end
      return found
    end
    return nil
  end

  # Remove the entry for the job ID <em>id</em>.
  def remove_entry(id)
    atrm_command=sprintf("%s %s",ATRM,id)
    begin
      system(atrm_command)
    rescue => err
      print($stderr,"ERROR trying to popen(#{atrm_command}): #{err}")
      exit(1)
    end 
  end
  
  # Remove all job entries in the queue.
  def clear
    @entries.each do |r,e|
      e.each do |job|
        self.remove_entry(job.jobid)
      end
    end
  end
end

# Class to represent INTEGRAL revolutions.
class Revolution
  def initialize(time,num)
    @time=time
    @num=num
  end
  
  # Perigee time (UTC)
  attr_reader :time
  # Revolution number
  attr_reader :num

  def to_s
    return sprintf("%s %s",num,time.asctime)
  end
end

# Generic job class.
class Job
  # Path to the "at" command
  AT='/usr/bin/at'
  # Format of the time string passed to "at"
  AT_FMT="%y%m%d%H%M"
  # The path to the job scripts which will be run by "at" (using at -f <jobscript>)
  JOB_SCRIPT_PATH="/home/isdc/iftsops/backup_scheduler/jobscript"
  # Job takes the following parameters:
  # [rev] Revolution object
  # [machine] Machine name
  # [offset] Offset in hours, relative to perigee time
  def initialize(rev,machine,offset=0)
    @revnum=rev.num
    @machine=machine
    # Apply offset in hours to start time (as sec):
    @attime = (rev.time + (offset * 3600)).strftime(AT_FMT)
  end

  def to_s
    return sprintf("JOB > R%s: machine %s attime: %s",@revnum,@machine,@attime)
  end
  
  # Submit the job using "at".
  def schedule
    @command=sprintf("export REVNO=#{@revnum}; export MACHINE=#{@machine}; %s -t %s -f %s/%s.job",AT,@attime,JOB_SCRIPT_PATH,@machine)
    # Run the at command:
    begin
      system(@command)
    rescue => err
      print($stderr,"ERROR: Problem queueing job: #{err}")
    end    
  end  
end




#--
# MAIN
#
# We expect to receive the revno file contents from STDIN.
# Entries look like this:
#
# 290 121  R  2002-10-17T05:47:25Z              START REVOLUTION 001        
#
revno_pattern=Regexp::compile('\s*\d+\s+121\s+[PR]\s+(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z\s+START\s+REVOLUTION\s+(\d+)\s*\Z')

# Read the configuration file. This file contains the machines which we want backed up, with an associated
# offset value:
config = YAML::load( File.open('/home/isdc/iftsops/backup_scheduler/config.yml') )

# Debugging flag obtained from config, so that the queue entries will be dumped to STDOUT once populated:
debug=config['debug']

# Current time. NB: all times are in UTC:
time_now_utc=Time.now.getutc
time_now_epsec=time_now_utc.tv_sec

# Array to store the list of revolutions for which we must schedule jobs:
revlist=Array::new()

# File to write the input content to:
received_file=File::open("/home/isdc/iftsops/backup_scheduler/received_files/revno_#{time_now_epsec}",'w')

# Scan each line:
STDIN.each do |line|
  # Write to local file:
  received_file.print(line)
  # For each entry, check to see if the perigee time is earlier than our current time. If so, continue.
  if md = revno_pattern.match(line)
    # Create time object for time of pergiee for this entry in the revno contents:
    perigee_time=Time.gm(md[1],md[2],md[3],md[4],md[5],md[6])
    # Only proceed with queueing a job if the perigee time is in the future. Store the 
    # revolutions for which we want to queue backup jobs:
    if perigee_time.tv_sec > time_now_epsec
      revlist << Revolution::new(perigee_time,md[7])
    end
  end
end

# Close file:
received_file.close()

# Query the current queues:
queue = Queue::new()
queue.populate()

# Dump if required, before jobs are added from this iteration (i.e. existing queue state):
if debug == 1
  queue.entries.sort.each do |rev,entries|
    printf("Rev (PRE) %s:\n",rev)
    entries.each do |e|
      printf("\t%s\n",e)
    end
  end
end

# Loop over the list of revolutions obtained from revno file contents:
revlist.each do |rev|
  # Loop over list of machines which backups should be scheduled for. This information
  # comes from the config.yml file: each machine specifies an offset which should be 
  # applied to shift the backup start time relative to perigee time:
  config['machines'].each do |machine|
    # See if there is/are already job(s) queued for this machine for this revolution:
    if jobs = queue.find(rev.num,machine['name'])
      # If there is, remove it:
      jobs.each { |job| queue.remove_entry(job.jobid) }
    end
    # Re-queue a new job:
    job = Job::new(rev,machine['name'],machine['offset'])
    job.schedule
  end
end

# POST Dump if required, before jobs are added from this iteration (i.e. existing queue state):
if debug == 1
  queue.entries.sort.each do |rev,entries|
    printf("Rev (POST) %s:\n",rev)
    entries.each do |e|
      printf("\t%s\n",e)
    end
  end
end
