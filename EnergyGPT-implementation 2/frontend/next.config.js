/** @type {import('next').NextConfig} */
module.exports = {
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'avatars.githubusercontent.com',
        port: '',
        pathname: '**'
      },
      {
        protocol: 'http',
        hostname: 'localhost',
        port: '8080',
        pathname: '/output/**'
      }
    ]
  }
}
