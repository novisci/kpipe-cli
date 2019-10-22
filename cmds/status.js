module.exports = {
  command: 'status',
  desc: 'Get status of cluster',
  // builder: (yargs) => yargs.commandDir('sources'),
  handler: (argv) => {
    console.info('Cluster status %s', argv.topic)
  }
}
