export function navigateToProvider(authorizationUrl: string): void {
  window.location.assign(authorizationUrl);
}