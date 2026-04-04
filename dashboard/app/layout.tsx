import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';
import Navbar from '@/components/Navbar';

const inter = Inter({
  variable: '--font-inter',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: 'The Bullpen — Texas CRE Intelligence',
  description: 'AI agent platform for Texas commercial real estate intelligence',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${inter.variable} h-full`}>
      <body className="min-h-full bg-gray-950 text-white font-[family-name:var(--font-inter)] antialiased">
        <div className="flex min-h-screen">
          <Navbar />
          <main className="flex-1 md:ml-0 mt-14 md:mt-0">
            <div className="p-4 md:p-6 lg:p-8">{children}</div>
          </main>
        </div>
      </body>
    </html>
  );
}
