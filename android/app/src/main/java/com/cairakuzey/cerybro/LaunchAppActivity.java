package com.cairakuzey.cerybro;

import android.app.Activity;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.content.pm.ResolveInfo;
import android.net.Uri;
import android.os.Bundle;
import android.widget.Toast;
import java.text.Normalizer;
import java.util.List;
import java.util.Locale;

/** Opens an installed launcher app by fuzzy name match. Invoked from the Cerybro web app. */
public class LaunchAppActivity extends Activity {
    @Override protected void onCreate(Bundle b) {
        super.onCreate(b);
        Uri u = getIntent().getData();
        String q = u == null ? null : u.getQueryParameter("name");
        if (q == null || q.trim().isEmpty()) { finish(); return; }
        PackageManager pm = getPackageManager();
        Intent main = new Intent(Intent.ACTION_MAIN).addCategory(Intent.CATEGORY_LAUNCHER);
        List<ResolveInfo> apps = pm.queryIntentActivities(main, 0);
        String nq = norm(q);
        ResolveInfo best = null; double bestScore = 0;
        for (ResolveInfo ri : apps) {
            if (ri.activityInfo.packageName.equals(getPackageName())) continue;
            String label = norm(String.valueOf(ri.loadLabel(pm)));
            double s = score(nq, label);
            String pkg = ri.activityInfo.packageName.toLowerCase(Locale.ROOT);
            if (nq.length() >= 3 && pkg.contains(nq.replace(" ", ""))) s = Math.max(s, 0.8);
            if (s > bestScore) { bestScore = s; best = ri; }
        }
        if (best != null && bestScore >= 0.6) {
            Intent l = pm.getLaunchIntentForPackage(best.activityInfo.packageName);
            if (l == null) l = new Intent(main).setClassName(best.activityInfo.packageName, best.activityInfo.name);
            l.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            try { startActivity(l); } catch (Exception e) { toast("Açılamadı: " + q); }
        } else {
            toast("Uygulama bulunamadı: " + q);
        }
        finish();
    }

    private void toast(String s) { Toast.makeText(getApplicationContext(), s, Toast.LENGTH_LONG).show(); }

    static String norm(String s) {
        s = s.replace('İ', 'i').replace('I', 'i').replace('ı', 'i').toLowerCase(new Locale("tr"));
        s = Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}+", "");
        return s.replaceAll("[^\\p{L}\\p{N} ]+", " ").replaceAll("\\s+", " ").trim();
    }

    static double score(String q, String label) {
        if (label.isEmpty()) return 0;
        if (label.equals(q)) return 1.0;
        String lq = label.replace(" ", ""), qq = q.replace(" ", "");
        if (lq.equals(qq)) return 0.98;
        if (label.startsWith(q) || lq.startsWith(qq)) return 0.9;
        for (String w : label.split(" ")) if (w.equals(q)) return 0.88;
        if (qq.length() >= 3 && lq.contains(qq)) return 0.8;
        int d = lev(qq, lq);
        return 1.0 - (double) d / Math.max(qq.length(), lq.length());
    }

    static int lev(String a, String b) {
        int[] p = new int[b.length() + 1], c = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) p[j] = j;
        for (int i = 1; i <= a.length(); i++) {
            c[0] = i;
            for (int j = 1; j <= b.length(); j++)
                c[j] = Math.min(Math.min(c[j - 1] + 1, p[j] + 1), p[j - 1] + (a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1));
            int[] t = p; p = c; c = t;
        }
        return p[b.length()];
    }
}
