import { VRMHumanBoneName } from '@pixiv/three-vrm'
import { EmotionType } from '@/features/messages/messages'

/**
 * ボーン回転オフセット定義（度数）
 * pitch: X軸 (正=下向き/指を曲げる, 負=上向き/指を反らす)
 * yaw:   Y軸 (正=左回転, 負=右回転)
 * roll:  Z軸 (正=右傾き/指を広げる, 負=左傾き)
 */
export type BoneRotationDeg = {
  bone: VRMHumanBoneName
  pitch: number
  yaw: number
  roll: number
}

export type GesturePose = ReadonlyArray<BoneRotationDeg>

// ヘルパー: 左右対称の指ジェスチャーを生成
const fingerCurl = (
  side: 'left' | 'right',
  proximalPitch: number,
  intermediatePitch: number,
  distalPitch: number
): BoneRotationDeg[] => {
  const fingers = ['Index', 'Middle', 'Ring', 'Little'] as const
  return fingers.flatMap((finger) => [
    {
      bone: `${side}${finger}Proximal` as VRMHumanBoneName,
      pitch: proximalPitch,
      yaw: 0,
      roll: 0,
    },
    {
      bone: `${side}${finger}Intermediate` as VRMHumanBoneName,
      pitch: intermediatePitch,
      yaw: 0,
      roll: 0,
    },
    {
      bone: `${side}${finger}Distal` as VRMHumanBoneName,
      pitch: distalPitch,
      yaw: 0,
      roll: 0,
    },
  ])
}

const bothHandsCurl = (
  proximalPitch: number,
  intermediatePitch: number,
  distalPitch: number
): BoneRotationDeg[] => [
  ...fingerCurl('left', proximalPitch, intermediatePitch, distalPitch),
  ...fingerCurl('right', proximalPitch, intermediatePitch, distalPitch),
]

/**
 * 感情ごとのジェスチャー定義
 * 体の角度は小さく（3-10度）、指はやや大きめ（10-60度）
 */
export const GESTURE_POSES: Readonly<Record<EmotionType, GesturePose>> = {
  neutral: [],
  happy: [
    // 体: 頭を上に、体を軽く反らす、腕やや外側
    { bone: 'head', pitch: -5, yaw: 0, roll: 3 },
    { bone: 'spine', pitch: -3, yaw: 0, roll: 0 },
    { bone: 'leftUpperArm', pitch: 0, yaw: 0, roll: -3 },
    { bone: 'rightUpperArm', pitch: 0, yaw: 0, roll: 3 },
    // 手: 開いた手（指を軽く伸ばす）
    ...bothHandsCurl(-5, -5, -3),
  ],
  sad: [
    // 体: うつむき、前かがみ、肩が前に落ちる
    { bone: 'head', pitch: 8, yaw: 0, roll: -3 },
    { bone: 'spine', pitch: 5, yaw: 0, roll: 0 },
    { bone: 'leftShoulder', pitch: 3, yaw: 0, roll: 0 },
    { bone: 'rightShoulder', pitch: 3, yaw: 0, roll: 0 },
    // 手: 力なく曲がる
    ...bothHandsCurl(15, 10, 8),
  ],
  angry: [
    // 体: 頭を前に、体が前のめり、胸を張る
    { bone: 'head', pitch: 5, yaw: 0, roll: 0 },
    { bone: 'spine', pitch: 3, yaw: 0, roll: 0 },
    { bone: 'chest', pitch: 2, yaw: 0, roll: 0 },
    // 手: 握り拳
    ...bothHandsCurl(50, 55, 45),
    { bone: 'leftThumbProximal', pitch: 30, yaw: 0, roll: 0 },
    { bone: 'rightThumbProximal', pitch: 30, yaw: 0, roll: 0 },
  ],
  relaxed: [
    // 体: 頭を横に傾け、リラックス
    { bone: 'head', pitch: 3, yaw: 0, roll: 5 },
    { bone: 'spine', pitch: -2, yaw: 0, roll: 2 },
    // 手: 自然に軽く曲げる
    ...bothHandsCurl(10, 8, 5),
  ],
  surprised: [
    // 体: 頭を後ろに引く、仰け反る
    { bone: 'head', pitch: -10, yaw: 0, roll: 0 },
    { bone: 'spine', pitch: -5, yaw: 0, roll: 0 },
    // 手: パッと開く（指を伸ばす）
    ...bothHandsCurl(-10, -8, -5),
    { bone: 'leftUpperArm', pitch: -5, yaw: 0, roll: -5 },
    { bone: 'rightUpperArm', pitch: -5, yaw: 0, roll: 5 },
  ],
}

/** Slerp 補間速度（大きいほど速い遷移）。フレームレート非依存。 */
export const GESTURE_SLERP_SPEED = 3.0

/** 呼吸オシレーション */
export const BREATHING = {
  bone: 'chest' as VRMHumanBoneName,
  amplitudeDeg: 1.5,
  frequencyHz: 0.25,
  axis: 'pitch' as const,
}

/** 微小なアイドル揺れ */
export const IDLE_SWAY = {
  bone: 'spine' as VRMHumanBoneName,
  amplitudeDeg: 0.8,
  frequencyHz: 0.1,
  axis: 'roll' as const,
}
