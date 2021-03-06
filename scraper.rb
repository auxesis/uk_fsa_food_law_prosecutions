require 'scraperwiki'
require 'geokit'
require 'spreadsheet'
require 'pry'

# Set an API key if provided
Geokit::Geocoders::GoogleGeocoder.api_key = ENV['MORPH_GOOGLE_API_KEY'] if ENV['MORPH_GOOGLE_API_KEY']

@mappings = {
  1  => 'food_business_operator',
  2  => 'trading_name',
  3  => 'defendant',
  4  => 'address',
  5  => 'postown',
  6  => 'county',
  7  => 'postcode',
  8  => 'offence_category',
  9  => 'offence_provision',
  10 => 'contravention_in_eu_regulations',
  11 => 'nature_of_offence',
  12 => 'date_of_conviction',
  13 => 'conviction_or_guilty_plea',
  14 => 'court_name',
  15 => 'region',
  16 => 'sentence',
  17 => 'costs_awarded',
  18 => 'prosecution_authority'
}

class String
  def to_md5
    Digest::MD5.new.hexdigest(self)
  end

  def scrub!
    self.gsub!(/[[:space:]]/, ' ') # convert all utf whitespace to simple space
    self.strip!
  end
end

def normalise_date(value)
  case
  when value.class == DateTime
    return value.to_date
  when value.class == String
    return Date.parse(value)
  when value.class == Date
    return value
  else
    puts "[debug] Unhandled date: #{value.inspect}"
    raise
  end
end

def normalise_costs_awarded(value)
  case
  when value.class == String
    normalise_string(value)
  when value.class == Float
    value
  else
    puts "[debug] Unhandled costs_awarded value: #{value.inspect}"
    raise
  end
end

def normalise_string(value)
  case value
  when nil
    nil
  when /^\s*N[\\\/]A\s*$/
    nil
  else
    value.strip
  end
end

def generate_id(details)
  return details.values.map(&:to_s).join(' ').to_md5
end

def build_prosecution(row)
  details = {}
  row.each_with_index do |value, index|
    key = @mappings[index]
    case key
    when nil
      next
    when 'date_of_conviction'
      value = normalise_date(value)
    when 'trading_name'
      value = normalise_string(value)
    when 'costs_awarded'
      value = normalise_costs_awarded(value)
    else
      # Remove all leading and trailing whitespace, remove unicode spaces
      value.scrub! if value.is_a? String
    end
    details.merge!({key => value})
  end
  details['link'] = "#{url}##{generate_id(details)}"
  return details
end

def geocode(prosecution)
  @addresses ||= {}

  address = prosecution['address']
  address = [
    prosecution['address'],
    prosecution['county'],
    prosecution['postcode'],
  ].join(', ')

  if @addresses[address]
    puts "Geocoding [cache hit] #{address}"
    location = @addresses[address]
  else
    puts "Geocoding #{address}"
    a = Geokit::Geocoders::GoogleGeocoder.geocode(address)

    if !a.lat && !a.lng
      a = Geokit::Geocoders::GoogleGeocoder.geocode(prosecution['postcode'])
    end

    location = {
      'lat' => a.lat,
      'lng' => a.lng,
    }

    @addresses[address] = location
  end

  prosecution.merge!(location)
end

def existing_record_ids
  return @cached if @cached
  @cached = ScraperWiki.select('link from data').map {|r| r['link']}
rescue SqliteMagic::NoSuchTable
  []
end

def url
  "https://www.food.gov.uk/sites/default/files/prosecution-outcomes.xls"
end

def fetch_prosecutions
  xls = open(url)

  sheet = Spreadsheet.open(xls).worksheet(0)
  header_row = sheet.row(5)
  max = sheet.rows.size - 1

  sheet.rows[6..max]
end

def main
  prosecutions = fetch_prosecutions
  prosecutions.map! { |p| build_prosecution(p) }

  puts "### Found #{prosecutions.size} notices"
  new_prosecutions = prosecutions.select {|r| !existing_record_ids.include?(r['link'])}
  puts "### There are #{new_prosecutions.size} new prosecutions"
  new_prosecutions.map! {|p| geocode(p) }

  # Serialise
  ScraperWiki.save_sqlite(['link'], new_prosecutions)

  puts "Done"
end

main()
