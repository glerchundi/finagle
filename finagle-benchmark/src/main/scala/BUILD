benchmark(name='scala',
  dependencies=[
    pants('3rdparty:caliper'),
    pants('3rdparty:scala-library'),
    pants('finagle/finagle-core'),
    pants('finagle/finagle-memcached'),
    pants('finagle/finagle-ostrich4'),
    pants('finagle/finagle-stats'),
    pants('finagle/finagle-zipkin'),
    pants('finagle/finagle-swift'),
    compiled_idl(
      idl_deps=pants('finagle/finagle-benchmark/src/main/thrift'),
      compiler='scrooge',
      language='scala',
      rpc_style='finagle'
    )
  ],
  sources=rglobs('*.scala')
)
