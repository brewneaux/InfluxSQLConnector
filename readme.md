# Influx SQL Connector

A connector that allows you to feed data into an InfluxDB instance straight from a SQL database.  

Currently, this only supports limited MySQL, but as it grows, it should support:
- more complex queries
- custom queries
- PostgreSQL

There is a lot todo...
- Create the application to generate the jobs config
- Write unit tests
- Figure out a way to make the application remember which point it inserted last (since, as of this commit, InfluxDB does not support `ORDER BY DESC`)
- Add PostgreSQL support

## Installation

Right now? Just clone and run.  Eventually, I will be making this more daemon-y, and it'll have an interactive configuration script to accompany it, which will spit out the line to add to your cron

## Usage

TODO: Write usage instructions

## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## History

TODO: Write history

## Credits

TODO: Write credits

## License

GPL 3.0.  See the `LICENSE` file.