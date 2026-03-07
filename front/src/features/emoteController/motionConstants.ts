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

/** 回転オシレーション定義 */
export type OscillationDef = {
  bone: VRMHumanBoneName
  amplitudeDeg: number
  frequencyHz: number
  axis: 'pitch' | 'yaw' | 'roll'
}

/** 感情ごとのモーション設定 */
export type EmotionMotionConfig = {
  oscillations: readonly OscillationDef[]
  slerpSpeed: number
}

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
 * 体の角度はやや大きめ（8-20度）、指はしっかり（10-60度）
 */
export const GESTURE_POSES: Readonly<Record<EmotionType, GesturePose>> = {
  neutral: [],
  happy: [
    // 体: 頭を上に、体を反らす、腕を外側に大きく開く
    { bone: 'head', pitch: -12, yaw: 0, roll: 5 },
    { bone: 'spine', pitch: -8, yaw: 0, roll: 0 },
    { bone: 'leftUpperArm', pitch: -5, yaw: 0, roll: -10 },
    { bone: 'rightUpperArm', pitch: -5, yaw: 0, roll: 10 },
    // 手: 大きく開いた手
    ...bothHandsCurl(-8, -8, -5),
  ],
  sad: [
    // 体: 軽くうつむき、やや前かがみ、肩が落ちる
    { bone: 'head', pitch: 5, yaw: 0, roll: -2 },
    { bone: 'spine', pitch: 3, yaw: 0, roll: 0 },
    { bone: 'leftShoulder', pitch: 6, yaw: 0, roll: 0 },
    { bone: 'rightShoulder', pitch: 6, yaw: 0, roll: 0 },
    // 手: 力なく曲がる
    ...bothHandsCurl(20, 15, 10),
  ],
  angry: [
    // 体: 頭を前に突き出し、前のめり、胸を張る
    { bone: 'head', pitch: 10, yaw: 0, roll: 0 },
    { bone: 'spine', pitch: 8, yaw: 0, roll: 0 },
    { bone: 'chest', pitch: 5, yaw: 0, roll: 0 },
    // 手: 強い握り拳
    ...bothHandsCurl(55, 60, 50),
    { bone: 'leftThumbProximal', pitch: 35, yaw: 0, roll: 0 },
    { bone: 'rightThumbProximal', pitch: 35, yaw: 0, roll: 0 },
  ],
  relaxed: [
    // 体: 頭を横に傾け、リラックス
    { bone: 'head', pitch: 5, yaw: 0, roll: 8 },
    { bone: 'spine', pitch: -3, yaw: 0, roll: 3 },
    // 手: 自然に軽く曲げる
    ...bothHandsCurl(12, 10, 7),
  ],
  surprised: [
    // 体: 軽く後ろに引く、体がこわばる
    { bone: 'head', pitch: -5, yaw: 0, roll: 0 },
    { bone: 'spine', pitch: -3, yaw: 0, roll: 0 },
    { bone: 'chest', pitch: -2, yaw: 0, roll: 0 },
    // 肩が少し上がる（こわばり）
    { bone: 'leftShoulder', pitch: -3, yaw: 0, roll: 0 },
    { bone: 'rightShoulder', pitch: -3, yaw: 0, roll: 0 },
    // 手: 指が軽く開く
    ...bothHandsCurl(-5, -3, -2),
  ],
}

// --- 基本オシレーション ---

/** 呼吸オシレーション */
export const BREATHING: OscillationDef = {
  bone: 'chest' as VRMHumanBoneName,
  amplitudeDeg: 1.5,
  frequencyHz: 0.25,
  axis: 'pitch',
}

/** 微小なアイドル揺れ */
export const IDLE_SWAY: OscillationDef = {
  bone: 'spine' as VRMHumanBoneName,
  amplitudeDeg: 0.8,
  frequencyHz: 0.1,
  axis: 'roll',
}

// --- 感情別モーション設定 ---

export const EMOTION_MOTION_CONFIG: Readonly<
  Record<EmotionType, EmotionMotionConfig>
> = {
  neutral: {
    oscillations: [BREATHING, IDLE_SWAY],
    slerpSpeed: 3.0,
  },
  happy: {
    oscillations: [
      BREATHING,
      // 大きめの横揺れ（楽しそうな体の振り）
      { bone: 'spine' as VRMHumanBoneName, amplitudeDeg: 3.0, frequencyHz: 0.4, axis: 'roll' },
      // 腕の軽い振り
      { bone: 'leftUpperArm' as VRMHumanBoneName, amplitudeDeg: 3.0, frequencyHz: 0.5, axis: 'roll' },
      { bone: 'rightUpperArm' as VRMHumanBoneName, amplitudeDeg: -3.0, frequencyHz: 0.5, axis: 'roll' },
    ],
    slerpSpeed: 5.0,
  },
  sad: {
    oscillations: [
      // ゆっくりした呼吸
      { bone: 'chest' as VRMHumanBoneName, amplitudeDeg: 1.0, frequencyHz: 0.15, axis: 'pitch' },
      // 小さくゆっくりした揺れ
      { bone: 'spine' as VRMHumanBoneName, amplitudeDeg: 0.5, frequencyHz: 0.08, axis: 'roll' },
    ],
    slerpSpeed: 1.5,
  },
  angry: {
    oscillations: [
      // 速い呼吸
      { bone: 'chest' as VRMHumanBoneName, amplitudeDeg: 2.0, frequencyHz: 0.5, axis: 'pitch' },
      // 体の前後揺れ（苛立ち）
      { bone: 'spine' as VRMHumanBoneName, amplitudeDeg: 1.5, frequencyHz: 0.6, axis: 'pitch' },
      // 頭の小さな震え（高周波）
      { bone: 'head' as VRMHumanBoneName, amplitudeDeg: 0.8, frequencyHz: 3.0, axis: 'yaw' },
    ],
    slerpSpeed: 6.0,
  },
  relaxed: {
    oscillations: [
      // ゆっくりした呼吸
      { bone: 'chest' as VRMHumanBoneName, amplitudeDeg: 1.2, frequencyHz: 0.18, axis: 'pitch' },
      // ゆったりとした横揺れ
      { bone: 'spine' as VRMHumanBoneName, amplitudeDeg: 1.2, frequencyHz: 0.08, axis: 'roll' },
    ],
    slerpSpeed: 2.0,
  },
  surprised: {
    oscillations: [
      BREATHING,
      IDLE_SWAY,
    ],
    slerpSpeed: 8.0,
  },
}
