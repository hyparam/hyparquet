import commonjs from '@rollup/plugin-commonjs'
import resolve from '@rollup/plugin-node-resolve'
import replace from '@rollup/plugin-replace'
import terser from '@rollup/plugin-terser'
import typescript from '@rollup/plugin-typescript'

export default [
  // demo bundle
  {
    input: 'demo/demo.js',
    output: {
      file: 'demo/bundle.min.js',
      format: 'umd',
      sourcemap: true,
    },
    plugins: [
      commonjs(),
      replace({
        'process.env.NODE_ENV': JSON.stringify('production'), // or 'development' based on your build environment
        preventAssignment: true,
      }),
      resolve({ browser: true }),
      terser(),
      typescript(),
    ],
  },
  // web worker
  {
    input: 'demo/workers/parquetWorker.js',
    output: {
      file: 'demo/workers/worker.min.js',
      format: 'umd',
      sourcemap: true,
    },
    plugins: [
      commonjs(),
      resolve({ browser: true }),
      terser(),
      typescript(),
    ],
  },
]
