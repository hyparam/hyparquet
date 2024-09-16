import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.js'

const app = document.getElementById('app')
if (!app) throw new Error('missing app element')

const params = new URLSearchParams(location.search)
const url = params.get('key') || undefined

// @ts-expect-error TODO: fix react createRoot type
const root = ReactDOM.createRoot(document.getElementById('app'))
root.render(React.createElement(App, { url }))
