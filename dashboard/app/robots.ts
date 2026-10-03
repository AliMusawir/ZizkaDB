import type { MetadataRoute } from 'next'
import { IS_SELF_HOSTED } from '@/lib/constants'

// OSS dashboard only — no operator /admin routes in this repo (see docs/REPO_SPLIT.md).
export default function robots(): MetadataRoute.Robots {
  // A self-hosted instance is private — nothing on it should be indexed.
  if (IS_SELF_HOSTED) return { rules: { userAgent: '*', disallow: '/' } }
  return {
    rules: {
      userAgent: '*',
      allow: '/',
      disallow: ['/dashboard', '/dashboard/'],
    },
  }
}
