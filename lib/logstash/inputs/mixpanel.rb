# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "mixpanel_client"
require "date"
require "time"

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Mixpanel < LogStash::Inputs::Base
  config_name "mixpanel"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # The API key of the project
  config :api_key, validate: :string, required: true

  # The Secret of the project
  config :api_secret, validate: :string, required: true

  # Set how frequently messages should be sent.
  #
  # The default, `1`, means send a message every second.
  config :schedule, validate: :string

  public
  def register
    require "rufus/scheduler"
    @client = Mixpanel::Client.new(api_key: @api_key, api_secret: @api_secret)
  end # def register

  def run(queue)
    if @schedule
      setup_scheduler(queue)
    else
      fetch(queue)
    end

  end # def run

  private

  def setup_scheduler(queue)
    @scheduler = Rufus::Scheduler.new
    @scheduler.cron(@schedule) do
      fetch(queue)
    end
    @scheduler.join
  end

  def fetch(queue)
    @client
      .request("export", from_date: (Date.today - 1).to_s, to_date: (Date.today - 1).to_s)
      .each do |raw_event|
        event = LogStash::Event.new raw_event
        event["@timestamp"] = LogStash::Timestamp.at(event["properties"]["time"])
        decorate(event)
        queue << event
      end
  end

end # class LogStash::Inputs::Example
