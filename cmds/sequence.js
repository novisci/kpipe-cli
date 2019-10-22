module.exports = {
  command: 'sequence <path> [options...]',
  desc: 'Execute a sequence located by path',

  builder:
    (yargs) => yargs
      .positional('path', {
        describe: 'Location of sequence module (js)'
      }),

  handler:
    (argv) => {
      require(`../../${argv.path}`)(argv)
        .then(console.info)
        .catch(console.error)
    }
}
